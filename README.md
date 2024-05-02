Start NATS srever with JetStream support:

```
nats-server -js
```

Fetch and publish blocks from Fuel Network. Use `latest` to start at the latest block height. Omit the parameter to start at the last block in the stream (which may be zero).

```
cargo run -- beta-5.fuel.network latest
```

Deploy and call a contract:

```
git clone https://github.com/lostman/fuel-counter
cd fuel-counter
make deploy && make call
```

Subscribe to NATS stream:

```
nats sub "receipts.*.topic_a" --headers-only --last
15:19:45 Subscribing to JetStream Stream holding messages with subject receipts.*.topic_a starting with the last message received
[#1] Received JetStream message: stream: fuel seq 168 / subject: receipts.11253361.topic_a / time: 2024-05-02T15:19:40+02:00
Nats-Msg-Size: 8
```
