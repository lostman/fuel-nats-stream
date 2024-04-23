use clap::Parser;

#[tokio::main]
async fn main() {
    let fuel_nats_stream::cli::Opt { url, height } = fuel_nats_stream::cli::Opt::parse();
    fuel_nats_stream::blocks::publisher(url.clone(), height)
        .await
        .unwrap();
}
