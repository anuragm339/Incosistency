package com.pos.inconsistency.controller;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

import java.util.Map;

/**
 * Simple liveness probe endpoint.
 *
 * Returns {@code {"status":"UP"}} so that load balancers and orchestrators
 * (e.g. Kubernetes liveness probes) can verify the service is responsive.
 *
 * Micronaut's built-in {@code /health} endpoint (via micronaut-management) provides
 * a richer health check with datasource and disk-space indicators.  This custom
 * endpoint provides a lightweight alternative at {@code /health}.
 */
@Controller("/health")
public class HealthController {

    /**
     * Returns a static UP status map.
     *
     * @return {@code {"status":"UP"}}
     */
    @Get
    public Map<String, String> health() {
        return Map.of("status", "UP");
    }
}
