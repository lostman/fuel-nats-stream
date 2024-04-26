use clap::Parser;
use fuel_nats_stream::blocks::last_processed_block_height;

#[tokio::main]
async fn main() {
    let fuel_nats_stream::cli::Opt { url, height } = fuel_nats_stream::cli::Opt::parse();

    let height = match height {
        // Start at a specific block height
        Some(fuel_nats_stream::cli::Height::Numeric(height)) => height,
        // Or latest
        Some(fuel_nats_stream::cli::Height::Latest) => {
            let provider = fuels::prelude::Provider::connect("beta-5.fuel.network")
                .await
                .unwrap();
            let ci = provider.chain_info().await.unwrap();
            ci.latest_block.header.height
        }
        // Or resume where we left off
        None => last_processed_block_height().await.unwrap_or(0),
    };

    fuel_nats_stream::blocks::publisher(url.clone(), height)
        .await
        .unwrap();
}
