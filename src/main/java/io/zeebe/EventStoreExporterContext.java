package io.zeebe;

import io.zeebe.exporter.api.context.Controller;
import org.slf4j.Logger;

public class EventStoreExporterContext {
    Controller controller;
    EventStoreExporterConfiguration configuration;
    Logger log;

    EventStoreExporterContext(Controller controller, EventStoreExporterConfiguration configuration, Logger log) {
        this.configuration = configuration;
        this.controller = controller;
        this.log = log;
    }
}
