use fuel_core_client::client::pagination::{PageDirection, PaginationRequest};
use fuel_core_client::client::FuelClient;
use fuel_core_client_ext::{ClientExt, FullBlock};

use async_stream::stream;

use futures::StreamExt;

/// Connect to a NATS server and publish messages
///   receipts.{kind}                      e.g. receipts.log_data
//    receipts.{contract_id}.{kind}
///   transactions.{height}.{index}.{kind} e.g. transactions.1.1.mint
///   blocks.{height}                      e.g. blocks.1
pub async fn publisher(url: String, start_block: u32) -> anyhow::Result<()> {
    // Connect to the NATS server
    let client = async_nats::connect("localhost:4222").await?;
    // Create a JetStream context
    let jetstream = async_nats::jetstream::new(client);
    // Create a JetStream stream
    let _stream = jetstream
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: "fuel".to_string(),
            subjects: vec![
                "blocks.*".to_string(),
                "receipts.*".to_string(),
                "receipts.*.*".to_string(),
                "transactions.*".to_string(),
            ],
            storage: async_nats::jetstream::stream::StorageType::File,
            ..Default::default()
        })
        .await?;
    // Stream Fuel blocks
    let block_stream = full_block_stream(url, start_block);
    futures::pin_mut!(block_stream);
    while let Some(block) = block_stream.next().await {
        let height = block.header.height.0;
        for (index, otx) in block.transactions.iter().enumerate() {
            for r in otx.receipts.as_ref().unwrap_or(&vec![]) {
                use fuel_core_client::client::schema::tx::transparent_receipt::ReceiptType;
                let receipt_kind = match r.receipt_type {
                    ReceiptType::Call => "call",
                    ReceiptType::Return => "return",
                    ReceiptType::ReturnData => "return_data",
                    ReceiptType::Panic => "panic",
                    ReceiptType::Revert => "revert",
                    ReceiptType::Log => "log",
                    ReceiptType::LogData => "log_data",
                    ReceiptType::Transfer => "transfer",
                    ReceiptType::TransferOut => "transfer_out",
                    ReceiptType::ScriptResult => "script_result",
                    ReceiptType::MessageOut => "message_out",
                    ReceiptType::Mint => "mint",
                    ReceiptType::Burn => "burn",
                };
                let payload = format!("{r:#?}");
                if let Some(contract_id) = &r.contract_id {
                    jetstream
                        .publish(
                            format!("receipts.{contract_id}.{receipt_kind}"),
                            payload.into(),
                        )
                        .await?;
                } else {
                    jetstream
                        .publish(format!("receipts.{receipt_kind}"), payload.into())
                        .await?;
                }
            }

            use fuel_core_types::fuel_types::canonical::Deserialize;
            let tx =
                fuel_core_types::fuel_tx::Transaction::decode(&mut otx.raw_payload.0 .0.as_slice())
                    .expect("Invalid tx from client");
            let tx_kind = match tx {
                fuel_core_types::fuel_tx::Transaction::Create(_) => "create",
                fuel_core_types::fuel_tx::Transaction::Mint(_) => "mint",
                fuel_core_types::fuel_tx::Transaction::Script(_) => "script",
            };
            let payload = format!("{tx:#?}");
            jetstream
                .publish(
                    format!("transactions.{height}.{index}.{tx_kind}"),
                    payload.into(),
                )
                .await?;
        }
        let payload = format!("{block:#?}");
        jetstream
            .publish(format!("blocks.{height}"), payload.into())
            .await?;
    }
    Ok(())
}

pub fn full_block_stream(url: String, start_block: u32) -> impl futures::Stream<Item = FullBlock> {
    let client = FuelClient::new(url).unwrap();
    let stream = futures::stream::unfold(
        (client, Some(start_block.to_string())),
        |(client, cursor)| async move {
            let page_req = PaginationRequest::<String> {
                cursor: cursor.clone(),
                results: 64,
                direction: PageDirection::Forward,
            };
            let req_start = std::time::Instant::now();
            println!("Client::full_blocks request");
            match client.full_blocks(page_req).await {
                Ok(result) => {
                    println!("Client::full_blocks reqeust in {:?}", req_start.elapsed());
                    Some((result.results, (client, result.cursor)))
                }
                // HTTP or other error. Keep trying.
                Err(err) => {
                    println!("Error: {err}");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    Some((vec![], (client, cursor)))
                }
            }
        },
    );
    stream! {
        for await blocks in stream {
            for block in blocks {
                yield block
            }
        }
    }
}