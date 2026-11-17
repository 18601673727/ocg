use super::{
    App, Arc, AtomicBool, AtomicUsize, DatabaseHandle, HttpServer, HttpServiceConfig, IoConfig,
    Ipv4Addr, MAX_HEADERS, NativeStops, Ordering, Seconds, Server, ServerConfig, Shared, SharedCfg,
    SocketAddr, TcpListener, UI_VARIABLE, answer, assets, browser, submissions, web,
};
use crate::system::logger::LogStore;

const STOP_TIMEOUT: Seconds = Seconds(5);

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("the API server cannot listen on 127.0.0.1:{port}: {source}")]
    Bind {
        port: u16,
        #[source]
        source: std::io::Error,
    },
    #[error("the API server cannot serve the frontend in {UI_VARIABLE}: {source}")]
    Ui {
        #[source]
        source: std::io::Error,
    },
    #[error("the API server could not start serving: {0}")]
    Serve(#[source] std::io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum StopError {
    #[error("the API server thread panicked")]
    Panicked,
    #[error("the API server failed while serving: {0}")]
    Serve(std::io::Error),
}

pub struct ApiServer {
    address: SocketAddr,
    server: Server,
    finished: ntex::rt::JoinHandle<std::io::Result<()>>,
    pub(super) shared: Arc<Shared>,
}

impl ApiServer {
    pub(crate) fn start(
        config: ServerConfig,
        database: DatabaseHandle,
        logs: Arc<LogStore>,
        native_stops: NativeStops,
        workspace_tools: crate::system::executor::execution_runtime::WorkspaceTools,
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
        let shared = Arc::new(Shared {
            database,
            logs,
            native_stops,
            submissions: submissions::Registry::default(),
            workspace_tools,
            token: config.token,
            port: address.port(),
            response_bytes: config.response_bytes,
            admission_wait: config.admission_wait,
            stream_limits: config.stream_limits,
            assets,
            browser: browser::State::default(),
            streams: AtomicUsize::new(0),
            closing: AtomicBool::new(false),
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
            SharedCfg::new("OCG-API").add(IoConfig::new()).add(
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

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub async fn stop(self) -> Result<(), StopError> {
        use ntex::util::{Either, select};
        self.shared.closing.store(true, Ordering::SeqCst);
        let mut finished = std::pin::pin!(self.finished);
        let joined = match select(self.server.stop(true), finished.as_mut()).await {
            Either::Left(()) => finished.await,
            Either::Right(joined) => joined,
        };
        joined
            .map_err(|_| StopError::Panicked)?
            .map_err(StopError::Serve)
    }
}
