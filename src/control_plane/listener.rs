use super::*;
use crate::data_directory::DataDirectory;

/// How long a stop waits for requests in flight before it closes them.
const STOP_TIMEOUT: Seconds = Seconds(5);

/// Why the listener did not start.
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("the control plane cannot listen on 127.0.0.1:{port}: {source}")]
    Bind {
        port: u16,
        #[source]
        source: std::io::Error,
    },
    #[error("the control plane cannot serve the frontend in {UI_VARIABLE}: {source}")]
    Ui {
        #[source]
        source: std::io::Error,
    },
    #[error("the control plane could not start serving: {0}")]
    Serve(#[source] std::io::Error),
}

/// Why the listener did not stop cleanly.
#[derive(Debug, thiserror::Error)]
pub enum StopError {
    #[error("the control plane thread panicked")]
    Panicked,
    #[error("the control plane failed while serving: {0}")]
    Serve(std::io::Error),
}

/// A running listener. Only [`ControlPlane::stop`] stops it.
pub struct ControlPlane {
    address: SocketAddr,
    server: Server,
    finished: ntex::rt::JoinHandle<std::io::Result<()>>,
    pub(super) shared: Arc<Shared>,
}

impl ControlPlane {
    /// Binds `127.0.0.1` at the configured port, then starts answering on
    /// a thread of its own. Nothing is answered unless the bind succeeded.
    /// Jobs are submitted to `scheduler`, which must be the host's own.
    /// A Project's root must stay apart from `data_directory`.
    ///
    /// # Errors
    ///
    /// Returns why the listener cannot be bound.
    pub fn start(
        config: ControlPlaneConfig,
        database: DatabaseHandle,
        scheduler: SchedulerHandle,
        data_directory: DataDirectory,
    ) -> Result<Self, StartError> {
        Self::start_with_workspace(
            config,
            database,
            scheduler,
            crate::orchestration::execution_runtime::WorkspaceTools::new(data_directory),
        )
    }

    pub(crate) fn start_with_workspace(
        config: ControlPlaneConfig,
        database: DatabaseHandle,
        scheduler: SchedulerHandle,
        workspace_tools: crate::orchestration::execution_runtime::WorkspaceTools,
    ) -> Result<Self, StartError> {
        #[cfg(test)]
        if ntex::rt::System::try_current().is_none() {
            return crate::test_support::on_system(move || async move {
                Self::start_with_workspace(config, database, scheduler, workspace_tools)
            });
        }

        let bind = |source| StartError::Bind {
            port: config.port,
            source,
        };
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, config.port)).map_err(bind)?;
        let address = listener.local_addr().map_err(bind)?;
        let assets = match config.ui_dir.as_deref() {
            Some(directory) => Some(
                assets::UiAssets::open(directory).map_err(|source| StartError::Ui { source })?,
            ),
            None => None,
        };
        // The browser surface is exactly the one the frontend serves in:
        // no UI directory, no session.
        let browser = assets
            .is_some()
            .then(browser::State::start)
            .transpose()
            .map_err(|_| StartError::Serve(std::io::Error::other("no randomness for sign-in")))?;
        let shared = Arc::new(Shared {
            database,
            scheduler,
            submissions: submissions::Registry::default(),
            workspace_tools,
            token: config.token,
            port: address.port(),
            response_bytes: config.response_bytes,
            admission_wait: config.admission_wait,
            stream_limits: config.stream_limits,
            assets,
            browser,
            streams: AtomicUsize::new(0),
            closing: AtomicBool::new(false),
            #[cfg(test)]
            fetched: AtomicUsize::new(0),
        });
        let kept = shared.clone();
        let server = HttpServer::new(async move || {
            App::new()
                .state(shared.clone())
                .default_service(web::to(answer))
        })
        .workers(1)
        .maxconn(64)
        .backlog(64)
        .shutdown_timeout(STOP_TIMEOUT)
        .config(
            SharedCfg::new("OCG-CONTROL").add(IoConfig::new()).add(
                HttpServiceConfig::new()
                    .set_max_headers(MAX_HEADERS)
                    .set_max_buf_size(16 * 1024),
            ),
        );
        let server = if ntex::rt::System::current().signals() {
            server
        } else {
            server.disable_signals()
        };
        let server = server.listen(listener).map_err(StartError::Serve)?.run();
        let finished = ntex::rt::spawn(server.clone());
        Ok(Self {
            address,
            server,
            finished,
            shared: kept,
        })
    }

    /// The address it listens on.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// The pairing code its operator signs a browser in with, when the
    /// browser surface is served. It is shown to the operator alone, and
    /// is not the bearer token.
    pub fn pairing_code(&self) -> Option<String> {
        self.shared
            .browser
            .as_ref()
            .map(|browser| browser.pairing_code().to_owned())
    }

    /// Stops accepting, lets requests in flight finish within a bounded
    /// time, then closes them and the listening socket, and joins the
    /// listener's thread. Event streams end at their next read, without a
    /// settlement, for their clients to resume. Admissions still running
    /// are dropped with it: none of them owns its Job, which the execution
    /// runtime settles or refuses as it stops, and whose record a later
    /// lookup reads.
    ///
    /// # Errors
    ///
    /// Returns [`StopError::Panicked`] when the listener's thread panicked.
    pub async fn stop(self) -> Result<(), StopError> {
        self.shared.closing.store(true, Ordering::SeqCst);
        self.server.stop(true).await;
        self.finished
            .await
            .map_err(|_| StopError::Panicked)?
            .map_err(StopError::Serve)
    }
}
