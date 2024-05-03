use std::str::FromStr;

use fuel_core_client::client::pagination::{PageDirection, PaginationRequest};
use fuel_core_client::client::FuelClient;
use fuel_core_client_ext::{ClientExt, FullBlock};

use async_stream::stream;
use futures::StreamExt;

const NUM_TOPICS: usize = 3;

// fuels::macros::abigen!(Contract(
//     name = "Counter",
//     abi = "../fuel-counter/contracts/counter/out/release/counter-abi.json"
// ));

pub async fn last_processed_block_height() -> anyhow::Result<u32> {
    let client = async_nats::connect("localhost:4222").await?;

    let jetstream = async_nats::jetstream::new(client);

    let consumer: async_nats::jetstream::consumer::PullConsumer = jetstream
        .get_stream("fuel")
        .await?
        // Then, on that `Stream` use method to create Consumer and bind to it too.
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            filter_subject: "blocks.*".to_string(),
            deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::Last,
            ..Default::default()
        })
        .await?;

    let message = consumer.stream().messages().await?.next().await;

    if let Some(Ok(message)) = message {
        let height = message.subject.split('.').last().unwrap();
        let height = height.parse::<u32>()?;
        Ok(height)
    } else {
        anyhow::bail!("No message found");
    }
}

/// Connect to a NATS server and publish messages
///   receipts.{height}.{kind}                         e.g. receipts.9000.log_data
//    receipts.{height}.{contract_id}.{kind}           e.g. receipts.9000.>
///   receipts.{height}.{topic_1}                      e.g. receipts.*.my_custom_topic
///   receipts.{height}.{topic_1}.{topic_2}            e.g. receipts.*.counter.inrc
///   receipts.{height}.{topic_1}.{topic_2}.{topic_3}
///   transactions.{height}.{index}.{kind}             e.g. transactions.1.1.mint
///   blocks.{height}                                  e.g. blocks.1
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
                // receipts.{height}.{topic_1}
                "receipts.*.*".to_string(),
                // receipts.{height}.{topic_1}.{topic_2}
                // or
                // receipts.{height}.{contract_id}.{kind}
                "receipts.*.*.*".to_string(),
                // receipts.{height}.{topic_1}.{topic_2}.{topic_3}
                "receipts.*.*.*.*".to_string(),
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
                // receipt topics, if any
                if let ReceiptType::LogData = r.receipt_type {
                    use fuel_core_client::client::schema::Bytes;
                    let data = r.data.as_ref().unwrap();
                    let data: &Bytes = &**data;
                    println!("Log Data Length: {}", data.len());
                    let header = Bytes::from_str("0x0000000012345678").unwrap();

                    if data.starts_with(&header.0) {
                        // With ABIDecoder
                        // let abi_decoder = ABIDecoder::new(DecoderConfig::default());
                        // let token = abi_decoder.decode(&topic::Topic::param_type(), &data.0)?;
                        // use fuels::core::traits::Tokenizable;
                        // let topic_msg = Topic::from_token(token)?;
                        // let topic_bytes = topic_msg.topic.0;
                        // let topic = String::from_utf8_lossy(&topic_bytes);

                        // Without ABIDecoder
                        let data = &data[header.0.len()..];
                        let mut topics = vec![];
                        for i in 0..NUM_TOPICS {
                            let topic_bytes: Vec<u8> = data[32 * i..32 * (i + 1)]
                                .iter()
                                .cloned()
                                .take_while(|x| *x > 0)
                                .collect();
                            let topic = String::from_utf8_lossy(&topic_bytes).into_owned();
                            if !topic.is_empty() {
                                topics.push(topic);
                            }
                        }
                        let topics = topics.join(".");
                        let payload = data[NUM_TOPICS * 32..].to_owned();

                        // Publish
                        println!("Publishing to topic: {}", format!("receipts.{height}.{topics}"));
                        jetstream
                            .publish(format!("receipts.{height}.{topics}"), payload.into())
                            .await?;
                    }
                }
                let payload = format!("{r:#?}");
                if let Some(contract_id) = &r.contract {
                    let contract_id = contract_id.id.clone();
                    let subject = format!("receipts.{height}.{contract_id}.{receipt_kind}");
                    jetstream.publish(subject, payload.into()).await?;
                } else {
                    let contract_id =
                        "0000000000000000000000000000000000000000000000000000000000000000";
                    jetstream
                        .publish(
                            format!("receipts.{height}.{contract_id}.{receipt_kind}"),
                            payload.into(),
                        )
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
            println!(
                "Client::full_blocks: cursor={}",
                cursor.clone().unwrap_or("0".to_string())
            );
            let req_start = std::time::Instant::now();
            let page_req = PaginationRequest::<String> {
                cursor: cursor.clone(),
                results: 256,
                direction: PageDirection::Forward,
            };
            match client.full_blocks(page_req).await {
                Ok(result) => {
                    println!(
                        "Client::full_blocks: completed in {:?}",
                        req_start.elapsed()
                    );
                    if result.cursor.is_none() {
                        println!("Client::full_blocks: reached the tip of the chain. Sleeping.");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        Some((result.results, (client, cursor)))
                    } else {
                        Some((result.results, (client, result.cursor)))
                    }
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
