use std::path::PathBuf;

use chooch::Choocher;

use reqwest::Url;
use structopt::StructOpt;
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;

#[derive(Debug, StructOpt)]
#[structopt(name = "chooch")]
struct Opt {
    #[structopt(name = "url")]
    url: Url,
    #[structopt(name = "output")]
    output: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    let choocher = Choocher::new(opt.url);
    let mut stream = choocher.stream().await?;

    let mut bytes_read = 0;

    let real_path = opt.output.clone();
    println!("final path: {}", &real_path.to_str().unwrap());
    {
        let mut tmp_path = opt.output.clone();
        tmp_path.set_extension(format!(
            "{}.choochdl",
            real_path.extension().unwrap().to_str().unwrap()
        ));
        tmp_path.set_file_name(format!(
            ".{}",
            real_path.file_name().unwrap().to_str().unwrap()
        ));

        println!("temp path: {}", &tmp_path.to_string_lossy());

        let mut output_file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
            .await?;

        while let Some(Some((data, is_last))) = stream.next().await {
            bytes_read += data.len();
            output_file.write_all(&data).await?;

            if is_last {
                println!("got last chunk");
                break;
            }
        }

        output_file.sync_all().await?;

        println!("all done! maybe wrote {} bytes.", bytes_read,);
        println!("renaming temp to {}", real_path.to_string_lossy());
        tokio::fs::rename(tmp_path, real_path).await?;
        println!("done! choocher has chooched.");
    }

    Ok(())
}
