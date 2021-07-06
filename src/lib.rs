use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use futures::stream::FuturesOrdered;
use reqwest::Url;
use tokio::{sync::mpsc, time::Instant};
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};

#[derive(Clone, Debug)]
pub struct Config {
    pub url: Url,
    pub concurrency: usize,
    pub chunk_size_bytes: usize,
}

#[derive(Clone)]
pub struct Choocher {
    client: reqwest::Client,
    config: Config,
    content_length: Option<usize>,
    rolling_chunk_download_times: Arc<Mutex<Vec<f64>>>,
}

fn new_client() -> reqwest::Client {
    reqwest::ClientBuilder::new()
        .pool_idle_timeout(Duration::from_millis(1000))
        .pool_max_idle_per_host(4)
        .build()
        .unwrap()
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
        Self {
            client: new_client(),
            config,
            content_length: None,
            rolling_chunk_download_times: Default::default(),
        }
    }

    /// Starts a Choocher with the default configuration and the configured url.
    pub async fn stream(mut self) -> anyhow::Result<impl Stream<Item = Option<(Bytes, bool)>>> {
        let max_concurrency = self.config.concurrency;
        let chunk_size = self.config.chunk_size_bytes;

        let (sink, output) = mpsc::channel(max_concurrency * 4);

        let content_len = self.content_length().await?;
        println!("Content Length: {}", content_len);

        let mut queue = FuturesOrdered::new();

        // sink data to the stream sequentially
        tokio::spawn(async move {
            let mut last_range = false;
            let (job_sink, mut job_queue) = mpsc::unbounded_channel();

            let spawn_worker =
                move |instance: Self, queue: &mut FuturesOrdered<_>, index, done_fn| {
                    let chunk_offset = index * chunk_size;
                    let mut range = (chunk_offset, chunk_offset + chunk_size - 1);
                    let mut is_last_range = false;
                    if range.1 >= content_len {
                        is_last_range = true;
                        range.1 = content_len - 1
                    }
                    println!("spawned chunk #{} range {:?}", index, range);
                    queue.push(tokio::spawn(instance.work(done_fn, range, is_last_range)));
                    return is_last_range;
                };

            let job_sink_inner = job_sink.clone();
            let sinker = async move {
                let mut idx = 0;
                loop {
                    let job_sink = job_sink_inner.clone();
                    let done_fn = move |duration: Duration| {
                        job_sink.send(Some(duration)).expect("couldn't send");
                    };
                    let rolling_chunk_times = self.rolling_chunk_download_times.clone();
                    let mut spawner = self.clone();
                    tokio::select! {
                        Some(duration) = job_queue.recv() => {
                            let mut recycle = false;
                            if let Some(time) = duration {
                                let secs = time.as_secs_f64();
                                println!("chunk downloaded in {} ({} MBps)", secs, (chunk_size as f64/ secs) / 1_000_000.0);
                                {
                                    let mut times_mut = rolling_chunk_times.lock().unwrap();


                                    let mut times_view = (*times_mut).clone();

                                    times_view.sort_by(|a, b| a.partial_cmp(b).unwrap());

                                    let len = times_view.len();
                                    if len > 0 {
                                        let mid = times_view.len() / 2;
                                        let middle = times_view[mid];


                                        // recycle connections that are slow
                                        if secs - middle > (max_concurrency / 2) as f64 {
                                            recycle = true;
                                        }
                                    }

                                    if times_mut.len() > max_concurrency * 2 {
                                        times_mut.pop();
                                    }

                                    times_mut.insert(0, secs);

                                }
                            }

                            if recycle {
                                spawner.client = new_client();
                            }

                            if !last_range {
                                if spawn_worker(spawner, &mut queue, idx, done_fn) {
                                    println!("last range spawned #{}", idx);
                                    last_range = true;
                                }
                                idx += 1;
                            }
                        }
                        Some(res) = queue.next() => {
                            if let Ok(Ok((bytes, is_last_range))) = res {
                                println!("flushing {} bytes", bytes.len());
                                sink.send(Some((bytes, is_last_range)))
                                    .await
                                    .expect("error sending to sink queue");
                            } else {
                                println!("Error! {:?}", res);
                                sink.send(None).await.expect("error finishing stream. sorry!");
                            }
                        }
                    }
                }
            };

            let watchdog = tokio::spawn(sinker);
            // warm the queue
            for _ in 0..max_concurrency {
                job_sink.send(None).expect("could not send new job");
                tokio::time::sleep(Duration::from_millis(750)).await;
            }

            watchdog.await.expect("error")
        });

        Ok(ReceiverStream::new(output))
    }

    async fn work(
        self: Self,
        mut done_fn: impl FnMut(Duration) -> (),
        range: (usize, usize),
        is_last: bool,
    ) -> anyhow::Result<(Bytes, bool)> {
        let start_time = Instant::now();
        let res = self
            .client
            .get(self.config.url)
            .header("Range", format!("bytes={}-{}", range.0, range.1))
            .send()
            .await?
            .error_for_status()?;

        let bytes = res.bytes().await?;

        done_fn(Instant::now() - start_time);

        Ok((bytes, is_last))
    }

    /// Returns (and the first time, fetches) the content length of the object in question.
    pub async fn content_length(&mut self) -> anyhow::Result<usize> {
        Ok(match self.content_length {
            Some(len) => len,
            None => {
                let len = self.fetch_content_length().await?;
                self.content_length.replace(len);
                len
            }
        })
    }

    /// Gets the content length from the configured URL using the HEAD method.
    async fn fetch_content_length(&self) -> anyhow::Result<usize> {
        let url = self.config.url.clone();
        let req = self.client.head(url).send().await?.error_for_status()?;

        let header = req
            .headers()
            .get("content-length")
            .expect("expected content-length header");

        let length = header.to_str()?.parse::<usize>()?;

        Ok(length)
    }
}
