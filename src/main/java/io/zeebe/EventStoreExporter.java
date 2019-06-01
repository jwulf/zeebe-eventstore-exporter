package io.zeebe;

import io.zeebe.exporter.api.context.Context;
import io.zeebe.exporter.api.context.Controller;
import io.zeebe.exporter.api.record.Record;
import io.zeebe.exporter.api.spi.Exporter;
import org.json.JSONArray;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.*;

public class EventStoreExporter implements Exporter
{
    private static final String ENV_PREFIX = "EVENT_STORE_EXPORTER_";
    private static final String ENV_URL = ENV_PREFIX + "URL";
    private static final String ENV_STREAM_NAME = ENV_PREFIX + "STREAM_NAME";
    private static final String ENV_BATCH_SIZE = ENV_PREFIX + "BATCH_SIZE";
    private static final String ENV_BATCH_TIME_MILLI = ENV_PREFIX + "BATCH_TIME_MILLI";

    private Logger log;
    private EventStoreExporterConfiguration configuration;

    private EventQueue eventQueue;
    private Batcher batcher;
    private Sender sender;
    private Controller controller;

    public void configure(final Context context) {
        log = context.getLogger();
        configuration = context
                .getConfiguration()
                .instantiate(EventStoreExporterConfiguration.class);
        applyEnvironmentVariables(configuration);

        configureRecordFilter(context);

        log.debug("Exporter configured with {}", configuration);
        testConnectionToDatabase(configuration);
    }

    private void configureRecordFilter(Context context) {
        RecordFilter filter = new RecordFilter();
        context.setFilter(filter);
    }

    private void testConnectionToDatabase(EventStoreExporterConfiguration configuration) {
        try {
            HttpStatelessSender connectionTest = new HttpStatelessSender(configuration);
            connectionTest.send(new JSONArray());
        } catch (Exception e) {
            throw new RuntimeException(e); // halt the broker if unavailable
        }
    }

    public void open(final Controller controller) {
        eventQueue = new EventQueue();
        batcher = new Batcher(configuration);
        sender = new Sender(controller, log, configuration);
        this.controller =  controller;
        controller.scheduleTask(Duration.ofMillis(batcher.batchPeriod), this::batchEvents);
        controller.scheduleTask(Duration.ofMillis(sender.sendPeriod), this::sendBatch);
        log.debug("Event Store exporter started.");
    }

    public void close() {
        log.debug("Closing Event Store Exporter");
    }

    public void export(Record record) {
        eventQueue.addEvent(record);
    }

    private void batchEvents() {
        batcher.batchFrom(eventQueue);
        controller.scheduleTask(Duration.ofMillis(batcher.batchPeriod), this::batchEvents);
    }

    private void sendBatch() {
        sender.sendFrom(batcher);
        controller.scheduleTask(Duration.ofMillis(sender.sendPeriod), this::sendBatch);
    }

    private void applyEnvironmentVariables(final EventStoreExporterConfiguration configuration) {
        final Map<String, String> environment = System.getenv();

        Optional.ofNullable(environment.get(ENV_STREAM_NAME))
                .ifPresent(streamName -> configuration.streamName = streamName);
        Optional.ofNullable(environment.get(ENV_URL))
                .ifPresent(url -> configuration.url = url);
        Optional.ofNullable(environment.get(ENV_BATCH_SIZE))
                .ifPresent(batchSize -> configuration.batchSize = Integer.parseInt(batchSize));
        Optional.ofNullable(environment.get(ENV_BATCH_TIME_MILLI))
                .ifPresent(batchTimeMilli -> configuration.batchTimeMilli = Integer.parseInt(batchTimeMilli));
    }
}
