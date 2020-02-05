# zeebe-eventstore-exporter
A Zeebe exporter to the [Event Store DB](https://eventstore.org/).

## Configuration

Configure the exporter via an `[[exporters]]]` entry in the `zeebe.cfg.toml` file:

```toml
[[exporters]]
id = "demo"
className = "io.zeebe.EventStoreExporter"
  [exporter.args]
  # batchTimeMilli = 300
  # batchSize = 100
  # streamName=zeebe
  # url=http://127.0.0.1:2113

```

Or via environment variables:

```bash
EVENT_STORE_EXPORTER_BATCH_TIME_MILLI=300
EVENT_STORE_EXPORTER_BATCH_SIZE=100
EVENT_STORE_EXPORTER_STREAM_NAME=zeebe
EVENT_STORE_EXPORTER_URL=http://localhost:2113
```

## Batching and Sending

The exporter pushes events into a queue as they are received from the broker.

The exporter runs two loops using the scheduler. The first loop batches the events from the queue into batches of the configured size configured. The second loop sends the batches to Event Store. If the connection to the Event Store database fails, this second loop implements a simple linear back-off until it recovers.

At the moment the two loops run at the same rate - apart from the scenario where the send loop is backing off. In an actual production scenario, you would code the loops depending on where the bottleneck in the system is. With the default settings (batch size: 100, batch time: 300ms), the exporter can export a maximum of 300 events / second.

# Docker

A `docker-compose` file to run the Zeebe broker with this exporter is available [here](https://github.com/zeebe-io/zeebe-docker-compose/tree/0.18).
