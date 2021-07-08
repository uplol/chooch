use std::path::PathBuf;

use chooch::Choocher;

use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use rand::Rng;
use reqwest::Url;
use structopt::StructOpt;
use tokio::io::AsyncWriteExt;

fn parse_bytes(src: &str) -> Result<usize, &'static str> {
    bytefmt::parse(src).map(|n| n as usize)
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "chooch",
    about = "Downloads files over HTTP using multiple streams"
)]
struct Opt {
    #[structopt(name = "url", help = "The URL you wish to download")]
    url: Url,
    #[structopt(name = "output", help = "The output destination for this download")]
    output: PathBuf,
    #[structopt(long = "chunk-size", short, default_value = "32MB", parse(try_from_str = parse_bytes))]
    chunk_size: usize,
    #[structopt(long = "workers", short, default_value = "6")]
    worker_count: usize,
    #[structopt(
        long = "force-overwrite",
        short = "f",
        help = "Overwrites existing output file if it already exists"
    )]
    force_overwrite: bool,
    #[structopt(
        long = "skip-prealloc",
        short = "s",
        help = "Skips the pre-allocation of the target file"
    )]
    skip_prealloc: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    let choocher = Choocher::new(opt.url, opt.chunk_size, opt.worker_count);

    let (content_length, mut chunks) = choocher.chunks().await?;

    let real_path = opt.output;
    let tmp_path = create_temp_path(&real_path, opt.force_overwrite)?;
    println!("final path: {}", &real_path.to_str().unwrap());
    println!("temp path: {}", &tmp_path.to_string_lossy());

    let mut output_file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_path)
        .await?;

    let skip_prealloc = opt.skip_prealloc;
    let task_res = tokio::spawn(async move {
        if !skip_prealloc {
            println!(
                "preallocating file ({})",
                bytefmt::format((content_length * 1024) as _)
            );
            output_file.set_len((content_length * 1024) as _).await?;
        }

        let mut bytes_written = 0;
        let bar = setup_progress_bar(content_length as u64);
        {
            while let Some(chunk) = chunks.next().await {
                output_file.write_all(&chunk).await?;
                bar.inc(chunk.len() as _);
                bytes_written += chunk.len();
            }
            output_file.flush().await?;
        }

        bar.finish();
        Ok(bytes_written)
    });

    let exit_signal = tokio::signal::ctrl_c();
    let res = async move {
        loop {
            tokio::select! {
                _ = exit_signal => {
                    return Err(anyhow::anyhow!("user-terminated via signal"));
                }
                res = task_res => {
                    return res?
                }
            }
        }
    };

    match res.await {
        Ok(bytes_written) => {
            println!("done! renaming to final destination...");
            tokio::fs::rename(tmp_path, &real_path).await?;
            println!(
                "{} bytes written to {}",
                bytes_written,
                real_path.to_string_lossy()
            );
            Ok(())
        }
        Err(e) => {
            println!("something went wrong. removing temp file...");
            tokio::fs::remove_file(tmp_path).await.unwrap();
            Err(e)
        }
    }
}

fn setup_progress_bar(length: u64) -> ProgressBar {
    let bar = ProgressBar::new_spinner();
    bar.set_length(length);
    bar.set_style(ProgressStyle::default_spinner().template("[{elapsed_precise}] {bar:40.cyan/blue} {bytes:>7}/{total_bytes:7} ({bytes_per_sec}, eta: {eta_precise})"));
    bar
}

fn create_temp_path(real_path: &PathBuf, overwrite: bool) -> anyhow::Result<PathBuf> {
    if real_path.exists() {
        if overwrite {
            println!(
                "warning: {} already exists, ovewriting...",
                &real_path.to_string_lossy()
            )
        } else {
            return Err(anyhow::anyhow!(
                "cannot overwrite destination file, use --force to force overwrite"
            ));
        }
    }
    let mut tmp_path = real_path.clone();
    tmp_path.set_file_name(format!(
        ".{}.choochdl~{}",
        real_path.file_name().unwrap().to_str().unwrap(),
        rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(6)
            .map(char::from)
            .collect::<String>()
    ));
    Ok(tmp_path)
}
