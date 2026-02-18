package com.pos.inconsistency;

import io.micronaut.runtime.Micronaut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the InconsistencyService Micronaut application.
 *
 * This service tracks POS message delivery across store clusters, detects
 * inconsistencies (duplicate keys, late responses, missing stores, etc.),
 * and emits telemetry events to New Relic.
 */
public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        log.info("Starting InconsistencyService...");
        Micronaut.run(Application.class, args);
    }
}
