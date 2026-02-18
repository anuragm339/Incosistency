package com.pos.inconsistency.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pos.inconsistency.model.MtsStore;
import com.pos.inconsistency.model.MtsSummary;
import com.pos.inconsistency.model.PosStatus;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Fire-and-forget service that emits custom events to the New Relic Insights Events API.
 *
 * All public methods catch all exceptions internally and log warnings — they never throw.
 * This ensures that NR telemetry failures never disrupt the core tracking logic.
 *
 * Event batches are capped at 2000 events per POST, which is the NR Insights API limit.
 */
@Singleton
public class NewRelicEmitService {

    private static final Logger log = LoggerFactory.getLogger(NewRelicEmitService.class);

    private static final int NR_BATCH_LIMIT = 2000;

    // Known POS statuses used when building PosTrackingResult events
    private static final String STATUS_DELIVERED_NO_ACK = "DELIVERED_NO_ACK";
    private static final String STATUS_FAILED           = "FAILED";
    private static final String STATUS_MISSING          = "MISSING";

    @Value("${newrelic.api-key}")
    private String apiKey;

    @Value("${newrelic.events-url}")
    private String eventsUrl;

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    @Inject
    public NewRelicEmitService(@Client HttpClient httpClient, ObjectMapper objectMapper) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
    }

    // -----------------------------------------------------------------------
    // Public event emitters
    // -----------------------------------------------------------------------

    /**
     * Emits a {@code MessageTrackingResult} event to NR representing the final
     * outcome of a fully-tracked message.
     *
     * @param summary               the finalized summary row
     * @param storeFinalizationTimes map of storeNumber -&gt; finalization timestamp
     *                               (may be empty; included as metadata)
     */
    public void emitMessageTrackingResult(MtsSummary summary,
                                           Map<String, Instant> storeFinalizationTimes) {
        try {
            Map<String, Object> event = new LinkedHashMap<>();
            event.put("eventType", "MessageTrackingResult");
            event.put("messageKey", summary.getMessageKey());
            event.put("clusterId", summary.getClusterId());
            event.put("msgOffset", summary.getMsgOffset());
            event.put("state", summary.getState());
            event.put("totalStores", summary.getTotalStores());
            event.put("storesDone", summary.getStoresDone());
            event.put("storesPartial", summary.getStoresPartial());
            event.put("storesTimedOut", summary.getStoresTimedOut());
            event.put("publishCount", summary.getPublishCount());
            event.put("inconsistencyCount", summary.getInconsistencies().size());
            event.put("inconsistencies", String.join(",", summary.getInconsistencies()));
            event.put("firstPublishedAt", toEpochMillis(summary.getFirstPublishedAt()));
            event.put("lastPublishedAt", toEpochMillis(summary.getLastPublishedAt()));
            event.put("expireAt", toEpochMillis(summary.getExpireAt()));
            event.put("trackingDurationMs", computeTrackingDuration(summary));
            event.put("timestamp", System.currentTimeMillis());

            // Optionally include store finalization timestamps as metadata
            if (storeFinalizationTimes != null && !storeFinalizationTimes.isEmpty()) {
                event.put("storeFinalizationCount", storeFinalizationTimes.size());
            }

            postEvents(List.of(event), "MessageTrackingResult");

        } catch (Exception e) {
            log.warn("Failed to emit MessageTrackingResult for messageKey={}: {}",
                    summary.getMessageKey(), e.getMessage());
        }
    }

    /**
     * Emits a {@code StoreTrackingResult} event representing the state of a store
     * at a particular completion checkpoint.
     *
     * @param store         the store row (may be mid-tracking or final)
     * @param checkpointPct the completion percentage that triggered this event
     */
    public void emitStoreTrackingResult(MtsStore store, int checkpointPct) {
        try {
            int expected = store.getExpectedPos() == null ? 0 : store.getExpectedPos().size();
            int responded = store.getRespondedPos() == null ? 0 : store.getRespondedPos().size();

            Map<String, Object> event = new LinkedHashMap<>();
            event.put("eventType", "StoreTrackingResult");
            event.put("messageKey", store.getMessageKey());
            event.put("clusterId", store.getClusterId());
            event.put("storeNumber", store.getStoreNumber());
            event.put("locationId", store.getLocationId());
            event.put("state", store.getState());
            event.put("checkpointPct", checkpointPct);
            event.put("expectedPosCount", expected);
            event.put("respondedPosCount", responded);
            event.put("missingPosCount", store.getMissingPos() == null ? 0 : store.getMissingPos().size());
            event.put("inconsistencyCount", store.getInconsistencies() == null ? 0 : store.getInconsistencies().size());
            event.put("inconsistencies", store.getInconsistencies() == null ? ""
                    : String.join(",", store.getInconsistencies()));
            event.put("expireAt", toEpochMillis(store.getExpireAt()));
            event.put("timestamp", System.currentTimeMillis());

            postEvents(List.of(event), "StoreTrackingResult");

        } catch (Exception e) {
            log.warn("Failed to emit StoreTrackingResult for messageKey={} store={}: {}",
                    store.getMessageKey(), store.getStoreNumber(), e.getMessage());
        }
    }

    /**
     * Emits one {@code PosTrackingResult} event per problematic POS machine
     * (MISSING, FAILED, DELIVERED_NO_ACK).
     *
     * Events are batched into groups of {@value #NR_BATCH_LIMIT} per POST call.
     *
     * @param store            the finalized store row
     * @param firstPublishedAt timestamp of the original publish (used to compute lag)
     */
    public void emitPosTrackingResults(MtsStore store, Instant firstPublishedAt) {
        try {
            List<Map<String, Object>> events = new ArrayList<>();

            // Emit events for POS machines that never responded (still in missingPos)
            if (store.getMissingPos() != null) {
                for (String posHost : store.getMissingPos()) {
                    events.add(buildPosEvent(store, posHost, STATUS_MISSING, null, firstPublishedAt));
                }
            }

            // Emit events for POS machines that responded but with a problematic status
            if (store.getPosStatuses() != null) {
                for (Map.Entry<String, PosStatus> entry : store.getPosStatuses().entrySet()) {
                    String posHost = entry.getKey();
                    PosStatus ps = entry.getValue();
                    if (ps.getStatus() == null) {
                        continue;
                    }
                    boolean isProblem = STATUS_FAILED.equals(ps.getStatus())
                            || STATUS_DELIVERED_NO_ACK.equals(ps.getStatus());
                    if (isProblem) {
                        events.add(buildPosEvent(store, posHost, ps.getStatus(),
                                ps.getPatchReceivedAt(), firstPublishedAt));
                    }
                }
            }

            if (events.isEmpty()) {
                return;
            }

            // Batch into NR_BATCH_LIMIT-sized chunks
            for (int i = 0; i < events.size(); i += NR_BATCH_LIMIT) {
                List<Map<String, Object>> batch = events.subList(i, Math.min(i + NR_BATCH_LIMIT, events.size()));
                postEvents(batch, "PosTrackingResult");
            }

        } catch (Exception e) {
            log.warn("Failed to emit PosTrackingResults for messageKey={} store={}: {}",
                    store.getMessageKey(), store.getStoreNumber(), e.getMessage());
        }
    }

    /**
     * Emits a {@code MessageTrackingResult} event with state=REPLACED for the
     * old summary that is being superseded by a re-publish.
     *
     * @param oldSummary the summary row that will be replaced
     */
    public void emitReplacedEvent(MtsSummary oldSummary) {
        try {
            // Clone the event but force state to REPLACED
            MtsSummary replaced = MtsSummary.builder()
                    .id(oldSummary.getId())
                    .messageKey(oldSummary.getMessageKey())
                    .clusterId(oldSummary.getClusterId())
                    .msgOffset(oldSummary.getMsgOffset())
                    .firstPublishedAt(oldSummary.getFirstPublishedAt())
                    .lastPublishedAt(oldSummary.getLastPublishedAt())
                    .expireAt(oldSummary.getExpireAt())
                    .totalStores(oldSummary.getTotalStores())
                    .storesDone(oldSummary.getStoresDone())
                    .storesPartial(oldSummary.getStoresPartial())
                    .storesTimedOut(oldSummary.getStoresTimedOut())
                    .publishCount(oldSummary.getPublishCount())
                    .state("REPLACED")
                    .inconsistencies(oldSummary.getInconsistencies())
                    .createdAt(oldSummary.getCreatedAt())
                    .updatedAt(oldSummary.getUpdatedAt())
                    .build();

            emitMessageTrackingResult(replaced, Map.of());

        } catch (Exception e) {
            log.warn("Failed to emit REPLACED event for messageKey={}: {}",
                    oldSummary.getMessageKey(), e.getMessage());
        }
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    private Map<String, Object> buildPosEvent(MtsStore store,
                                               String posHost,
                                               String status,
                                               Instant patchReceivedAt,
                                               Instant firstPublishedAt) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("eventType", "PosTrackingResult");
        event.put("messageKey", store.getMessageKey());
        event.put("clusterId", store.getClusterId());
        event.put("storeNumber", store.getStoreNumber());
        event.put("locationId", store.getLocationId());
        event.put("posHostname", posHost);
        event.put("status", status);
        event.put("expireAt", toEpochMillis(store.getExpireAt()));
        event.put("firstPublishedAt", toEpochMillis(firstPublishedAt));

        if (patchReceivedAt != null && firstPublishedAt != null) {
            long lagMs = ChronoUnit.MILLIS.between(firstPublishedAt, patchReceivedAt);
            event.put("responselagMs", lagMs);
        }

        event.put("timestamp", System.currentTimeMillis());
        return event;
    }

    /**
     * Serializes the event list to JSON and POSTs to the NR Events API.
     * Uses a blocking reactive call — acceptable because this is fire-and-forget on a
     * background thread and NR calls should complete quickly.
     *
     * @param events    list of event maps to POST
     * @param eventType label used only for logging
     */
    private void postEvents(List<Map<String, Object>> events, String eventType) {
        if (events == null || events.isEmpty()) {
            return;
        }
        try {
            String payload = objectMapper.writeValueAsString(events);

            HttpRequest<String> request = HttpRequest.POST(eventsUrl, payload)
                    .contentType(MediaType.APPLICATION_JSON_TYPE)
                    .header("X-Insert-Key", apiKey);

            HttpResponse<String> response = httpClient.toBlocking().exchange(request, String.class);
            log.info("NR {} POST status={} events={}", eventType, response.getStatus().getCode(), events.size());

        } catch (Exception e) {
            log.warn("NR POST failed for eventType={} events={}: {}", eventType, events.size(), e.getMessage());
        }
    }

    private static long toEpochMillis(Instant instant) {
        return instant == null ? 0L : instant.toEpochMilli();
    }

    private static long computeTrackingDuration(MtsSummary summary) {
        if (summary.getFirstPublishedAt() == null) {
            return 0L;
        }
        Instant end = summary.getUpdatedAt() != null ? summary.getUpdatedAt() : Instant.now();
        return ChronoUnit.MILLIS.between(summary.getFirstPublishedAt(), end);
    }
}
