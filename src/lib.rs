use std::{
    collections::BTreeSet,
    ops::Range,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::Bytes;
use dynamic_pool::{DynamicPool, DynamicReset};
use futures::{FutureExt, Stream, StreamExt};
use reqwest::{Client, Url};

#[derive(Clone, Debug)]
pub struct Config {
    pub url: Url,
    pub concurrency: usize,
    pub chunk_size_bytes: usize,
}

#[derive(Clone)]
pub struct Choocher {
    worker_pool: DynamicPool<ChoocherWorker>,
    config: Config,
    slowest_worker_killer: Arc<Mutex<ChoocherWorkerKiller>>,
}

impl Choocher {
    /// Creates a new Choocher instance given the URL, uses an optimal default configuration.
    pub fn new(url: Url) -> Self {
        Self::new_with_config(Config {
            url,
            concurrency: 12,
            chunk_size_bytes: 1024 * 1024 * 128, // 8 MB
        })
    }

    // Creates a new Choocher instance, allowing the caller to specify a detailed config.
    pub fn new_with_config(config: Config) -> Self {
        let concurrency = config.concurrency;

        Self {
            worker_pool: DynamicPool::new(0, concurrency, || ChoocherWorker::new()),
            config,
            slowest_worker_killer: Arc::new(Mutex::new(ChoocherWorkerKiller::new(concurrency))),
        }
    }

    pub async fn chunks(self) -> anyhow::Result<impl Stream<Item = Bytes>> {
        let content_length = self.content_length().await?;
        let chunks = self.chunks_for_content_length(content_length);
        let url = self.config.url.clone();
        let pool = self.worker_pool.clone();
        let slowest_worker_killer = self.slowest_worker_killer.clone();

        Ok(futures::stream::iter(chunks)
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
            .buffered(self.config.concurrency))
    }

    fn chunks_for_content_length(&self, content_length: usize) -> Vec<Range<usize>> {
        let mut chunks = Vec::with_capacity(content_length / self.config.chunk_size_bytes);
        let mut last_end = 0;

        while last_end < content_length {
            let start = last_end;
            last_end = (start + self.config.chunk_size_bytes).min(content_length);
            chunks.push(start..last_end - 1);
        }

        chunks
    }

    /// Gets the content length from the configured URL using the HEAD method.
    async fn content_length(&self) -> anyhow::Result<usize> {
        let worker = self.worker_pool.take();
        let request = worker
            .client()
            .head(self.config.url.clone())
            .send()
            .await?
            .error_for_status()?;

        let header = request
            .headers()
            .get("content-length")
            .expect("expected content-length header");

        let length = header.to_str()?.parse::<usize>()?;

        Ok(length)
    }
}

struct ChoocherWorker {
    client: Client,
}

impl ChoocherWorker {
    fn new() -> Self {
        let client = reqwest::ClientBuilder::new()
            .pool_idle_timeout(Duration::from_millis(1000))
            .pool_max_idle_per_host(1)
            .build()
            .unwrap();

        Self { client }
    }

    fn client(&self) -> &Client {
        &self.client
    }

    async fn fetch_chunk(&self, url: Url, range: Range<usize>) -> anyhow::Result<Bytes> {
        let res = self
            .client
            .get(url)
            .header("Range", format!("bytes={}-{}", range.start, range.end))
            .send()
            .await?
            .error_for_status()?;

        let bytes = res.bytes().await?;
        Ok(bytes)
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
