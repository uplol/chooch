mod client;

use std::{
    collections::BTreeSet,
    net::IpAddr,
    ops::Range,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::Bytes;
use dynamic_pool::{DynamicPool, DynamicReset};
use futures::{FutureExt, Stream, StreamExt};
use hyper::{Body, Uri};

use crate::client::UniquePortClient;

#[derive(Clone, Debug)]
pub struct Config {
    pub url: Uri,
    pub concurrency: usize,
    pub chunk_size_bytes: usize,
    pub bind_ip: Option<IpAddr>,
}

#[derive(Clone)]
pub struct Choocher {
    worker_pool: DynamicPool<ChoocherWorker>,
    config: Config,
    slowest_worker_killer: Arc<Mutex<ChoocherWorkerKiller>>,
}

impl Choocher {
    /// Creates a new Choocher instance given the URL, uses an optimal default configuration.
    pub fn new(url: Uri, chunk_size: usize, worker_count: usize, bind_ip: Option<IpAddr>) -> Self {
        Self::new_with_config(Config {
            url,
            concurrency: worker_count,
            chunk_size_bytes: chunk_size,
            bind_ip,
        })
    }

    // Creates a new Choocher instance, allowing the caller to specify a detailed config.
    pub fn new_with_config(config: Config) -> Self {
        let concurrency = config.concurrency;
        let bind_ip = config.bind_ip;
        Self {
            worker_pool: DynamicPool::new(0, concurrency, move || {
                ChoocherWorker::new(bind_ip.clone())
            }),
            config,
            slowest_worker_killer: Arc::new(Mutex::new(ChoocherWorkerKiller::new(concurrency))),
        }
    }

    pub async fn chunks(self) -> anyhow::Result<(usize, impl Stream<Item = Bytes>)> {
        let content_length = self.content_length().await? as usize;
        let chunks = self.chunks_for_content_length(content_length);
        let url = self.config.url.clone();
        let pool = self.worker_pool.clone();
        let slowest_worker_killer = self.slowest_worker_killer.clone();
        let chunk_stream = futures::stream::iter(chunks)
            .map(move |chunk| {
                let url = url.clone();
                let worker = pool.take();
                let download_duration_tracker = slowest_worker_killer.clone();

                // Spawn the download in its own task, so that we just have to poll the task completion, and can
                // actually use many cores for the job.
                let handle = tokio::spawn(async move {
                    loop {
                        let start = Instant::now();

                        if let Ok(bytes) = worker.fetch_chunk(url.clone(), chunk.clone()).await {
                            let elapsed = start.elapsed();

                            // If we are the slowest download, we'll detach the client, forcing the pool to create a
                            // new client in the future.
                            if download_duration_tracker
                                .lock()
                                .unwrap()
                                .should_discard_client(elapsed)
                            {
                                worker.detach();
                            }
                            return bytes;
                        }
                    }
                });

                handle.map(|x| x.unwrap())
            })
            .buffered(self.config.concurrency);

        Ok((content_length, chunk_stream))
    }

    fn chunks_for_content_length(&self, content_length: usize) -> Vec<Range<usize>> {
        let mut chunks =
            Vec::with_capacity((1 + (content_length / self.config.chunk_size_bytes)) as _);
        let mut last_end = 0;

        while last_end < content_length {
            let start = last_end;
            last_end = (start + self.config.chunk_size_bytes).min(content_length);
            chunks.push(start..last_end - 1);
        }

        chunks
    }

    /// Gets the content length from the configured URL using the HEAD method.
    async fn content_length(&self) -> anyhow::Result<u64> {
        let worker = self.worker_pool.take();
        let client = worker.client();
        let req = UniquePortClient::new_request(self.config.url.clone())
            .method("HEAD")
            .body(Body::empty())?;
        let res = client.request(req).await?;
        if !res.status().is_success() {
            return Err(anyhow::anyhow!(format!("error status {}", res.status())));
        }
        let header = res
            .headers()
            .get("content-length")
            .expect("expected content-length header");
        Ok(header.to_str()?.parse()?)
    }
}

struct ChoocherWorker {
    client: UniquePortClient,
}

impl ChoocherWorker {
    fn new(bind_ip: Option<IpAddr>) -> Self {
        Self {
            client: UniquePortClient::new(bind_ip),
        }
    }

    fn client(&self) -> &UniquePortClient {
        &self.client
    }

    async fn fetch_chunk(&self, url: Uri, range: Range<usize>) -> anyhow::Result<Bytes> {
        let req = UniquePortClient::new_request(url)
            .header("Range", format!("bytes={}-{}", range.start, range.end))
            .body(Body::empty())?;
        let res = self.client.request(req).await?;
        if !res.status().is_success() {
            return Err(anyhow::anyhow!(format!("error status {}", res.status())));
        }
        Ok(hyper::body::to_bytes(res.into_body()).await?)
    }
}

impl DynamicReset for ChoocherWorker {
    fn reset(&mut self) {
        // nothing to do here.
    }
}

struct ChoocherWorkerKiller {
    target_concurrency: usize,
    timings: BTreeSet<Duration>,
}

impl ChoocherWorkerKiller {
    fn new(target_concurrency: usize) -> Self {
        assert!(target_concurrency > 0);
        Self {
            target_concurrency,
            timings: BTreeSet::new(),
        }
    }

    fn should_discard_client(&mut self, download_time: Duration) -> bool {
        // We don't have enough samples yet.
        if self.timings.len() < self.target_concurrency {
            self.timings.insert(download_time);
            return false;
        }

        let slowest_download_time = self
            .timings
            .iter()
            .rev()
            .next()
            .expect("invariant: should have some timings.")
            .clone();

        self.timings.remove(&slowest_download_time);
        self.timings.insert(download_time);

        // We'll track the last N many chunk downloads, and reject clients if they exceed the slowest download time repeatedly.
        download_time > slowest_download_time
    }
}
