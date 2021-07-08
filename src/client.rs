use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU16, Ordering};
use std::task::Poll;

use futures::future::BoxFuture;
use hyper::http::request;
use hyper::service::Service;
use hyper::{Request, Uri};
use tokio::net::{TcpSocket, TcpStream};
use trust_dns_resolver::config::{ResolverConfig, ResolverOpts};
use trust_dns_resolver::TokioAsyncResolver;

static NEXT_PORT: AtomicU16 = AtomicU16::new(1024);
pub fn next_port() -> u16 {
    let cur = NEXT_PORT.load(Ordering::SeqCst);
    if cur >= u16::MAX {
        NEXT_PORT.swap(1024, Ordering::SeqCst)
    } else {
        NEXT_PORT.swap(cur + 1, Ordering::SeqCst)
    }
}

static APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

#[derive(Clone)]
pub struct UniquePortClient {
    inner: hyper::client::Client<ChoochConnector>,
}

impl UniquePortClient {
    pub fn new(bind_ip: Option<IpAddr>) -> Self {
        Self {
            inner: hyper::Client::builder().build::<_, hyper::Body>(ChoochConnector { bind_ip }),
        }
    }

    pub fn new_request(uri: Uri) -> request::Builder {
        Request::builder()
            .header("User-Agent", APP_USER_AGENT)
            .uri(uri)
    }
}

impl std::ops::Deref for UniquePortClient {
    type Target = hyper::client::Client<ChoochConnector>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for UniquePortClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[derive(Clone, Default)]
pub struct ChoochConnector {
    bind_ip: Option<IpAddr>,
}

impl Service<Uri> for ChoochConnector {
    type Response = TcpStream;
    type Error = anyhow::Error;
    type Future = BoxFuture<'static, std::result::Result<TcpStream, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Uri) -> Self::Future {
        let bind_ip = self.bind_ip.clone();
        Box::pin(async move {
            let host = req
                .host()
                .ok_or_else(|| anyhow::anyhow!("missing host from uri"))?;

            let ip = match host.parse().ok() {
                Some(ip) => ip,
                None => {
                    let resolver = TokioAsyncResolver::tokio(
                        ResolverConfig::default(),
                        ResolverOpts::default(),
                    )?;
                    let response = resolver.lookup_ip(host).await?;
                    response
                        .into_iter()
                        .next()
                        .ok_or_else(|| anyhow::anyhow!(format!("error resolving host {}", host)))?
                }
            };

            let (bind_ip, socket) = match ip {
                IpAddr::V4(_) => (
                    bind_ip.unwrap_or_else(|| "0.0.0.0".parse().unwrap()),
                    TcpSocket::new_v4()?,
                ),
                IpAddr::V6(_) => (
                    bind_ip.unwrap_or_else(|| "[::]".parse().unwrap()),
                    TcpSocket::new_v6()?,
                ),
            };

            loop {
                // try to bind to the next available incremental port
                let next_port = next_port();
                if socket
                    .bind(match ip {
                        IpAddr::V4(_) => SocketAddr::new(bind_ip, next_port),
                        IpAddr::V6(_) => SocketAddr::new(bind_ip, next_port),
                    })
                    .is_ok()
                {
                    break;
                }
            }

            let target_socket_addr = SocketAddr::new(ip, req.port_u16().unwrap_or(80));
            Ok(socket.connect(target_socket_addr).await?)
        })
    }
}
