use super::*;

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
}

/// A running listener. Only [`ControlPlane::stop`] stops it.
pub struct ControlPlane {
    address: SocketAddr,
    server: Server,
    thread: std::thread::JoinHandle<()>,
    pub(super) shared: Arc<Shared>,
}

impl ControlPlane {
    /// Binds `127.0.0.1` at the configured port, then starts answering on
    /// a thread of its own. Nothing is answered unless the bind succeeded.
    /// Jobs are submitted to `scheduler`, which must be the host's own.
    ///
    /// # Errors
    ///
    /// Returns why the listener cannot be bound.
    pub fn start(
        config: ControlPlaneConfig,
        database: DatabaseHandle,
        scheduler: SchedulerHandle,
    ) -> Result<Self, StartError> {
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
        let (started, starting) = flume::bounded(1);
        let thread = std::thread::Builder::new()
            .name("ocg-control-plane".to_owned())
            .spawn(move || {
                // A runtime of its own, without signals: the server's
                // signal settings apply to this runtime only.
                ntex::rt::System::new("ocg-control-plane", ntex::rt::DefaultRuntime).block_on(
                    async move {
                        let server = HttpServer::new(async move || {
                            App::new()
                                .state(shared.clone())
                                .default_service(web::to(answer))
                        })
                        .workers(1)
                        .maxconn(64)
                        .backlog(64)
                        .disable_signals()
                        .shutdown_timeout(STOP_TIMEOUT)
                        .config(
                            SharedCfg::new("OCG-CONTROL").add(IoConfig::new()).add(
                                HttpServiceConfig::new()
                                    .set_max_headers(MAX_HEADERS)
                                    .set_max_buf_size(16 * 1024),
                            ),
                        )
                        .listen(listener);
                        match server {
                            Ok(server) => {
                                let server = server.run();
                                started.send(Ok(server.clone())).ok();
                                // Resolves once the server has stopped.
                                server.await.ok();
                            }
                            Err(error) => {
                                started.send(Err(error)).ok();
                            }
                        }
                    },
                );
            })
            .map_err(StartError::Serve)?;
        let server = match starting.recv() {
            Ok(Ok(server)) => server,
            Ok(Err(error)) => {
                thread.join().ok();
                return Err(StartError::Serve(error));
            }
            Err(_) => {
                thread.join().ok();
                return Err(StartError::Serve(std::io::Error::other(
                    "the control plane thread stopped before serving",
                )));
            }
        };
        Ok(Self {
            address,
            server,
            thread,
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
        self.thread.join().map_err(|_| StopError::Panicked)
    }
}
