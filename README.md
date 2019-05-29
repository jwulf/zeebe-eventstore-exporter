# zeebe-eventstore-exporter
A Zeebe exporter to the [Event Store DB](https://eventstore.org/).

## Configuration

Configure the exporter via an `[[exporters]]]` entry in the `zeebe.cfg.toml` file:

```toml
[[exporters]]
id = "demo"
className = "io.zeebe.EventStoreExporter"
# batchTimeMilli = 300
# batchSize = 100
# retryTimeMilli = 500
# streamName=zeebe
# url=http://127.0.0.1:2113

```

Or via environment variables:

```bash
EVENT_STORE_EXPORTER_BATCH_TIME_MILLI=300
EVENT_STORE_EXPORTER_BATCH_SIZE=100
EVENT_STORE_EXPORTER_RETRY_TIME_MILLI=300
EVENT_STORE_EXPORTER_STREAM_NAME=zeebe
EVENT_STORE_EXPORTER_URL=http://localhost:2113
```