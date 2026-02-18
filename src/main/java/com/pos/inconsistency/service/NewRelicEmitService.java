package com.pos.inconsistency.service;

import com.newrelic.telemetry.Attributes;
import com.newrelic.telemetry.OkHttpPoster;
import com.newrelic.telemetry.SenderConfiguration;
import com.newrelic.telemetry.events.Event;
import com.newrelic.telemetry.events.EventBatch;
import com.newrelic.telemetry.events.EventBatchSender;
import com.pos.inconsistency.model.MtsStore;
import com.pos.inconsistency.model.MtsSummary;
import com.pos.inconsistency.model.PosStatus;
import io.micronaut.context.annotation.Value;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Fire-and-forget service that emits custom events to New Relic via the Telemetry SDK.
 *
 * All public methods catch all exceptions internally and log warnings — they never throw.
 * This ensures that NR telemetry failures never disrupt the core tracking logic.
 *
 * Event batches are capped at 2000 events per send, which is the NR Insights API limit.
 */
@Singleton
public class NewRelicEmitService {

    private static final Logger log = LoggerFactory.getLogger(NewRelicEmitService.class);

    private static final int NR_BATCH_LIMIT = 2000;

    private static final String STATUS_DELIVERED_NO_ACK = "DELIVERED_NO_ACK";
    private static final String STATUS_FAILED           = "FAILED";
    private static final String STATUS_MISSING          = "MISSING";

    @Value("${newrelic.api-key}")
    private String apiKey;

    @Value("${newrelic.account-id}")
    private String accountId;

    /** Number of worker threads draining the queue. Tune via NR_WORKER_THREADS env var. */
    @Value("${newrelic.worker-threads:4}")
    private int workerThreadCount;

    /** Bounded queue capacity. Tune via NR_QUEUE_CAPACITY env var. */
    @Value("${newrelic.queue-capacity:50000}")
    private int queueCapacity;

    private EventBatchSender eventBatchSender;

    /**
     * Bounded queue of ready-to-send event batches.
     * Initialised in {@link #init()} after config values are injected.
     * Request threads only ever call {@link LinkedBlockingQueue#offer} — non-blocking.
     * Worker threads drain it via {@link #drainQueue(int)}.
     */
    volatile LinkedBlockingQueue<EventBatch> eventQueue;

    @PostConstruct
    void init() {
        SenderConfiguration config = SenderConfiguration
                .builder("insights-collector.newrelic.com",
                        "/v1/accounts/" + accountId + "/events")
                .apiKey(apiKey)
                .httpPoster(new OkHttpPoster())
                .build();
        eventBatchSender = EventBatchSender.create(config);

        eventQueue = new LinkedBlockingQueue<>(queueCapacity);

        for (int i = 0; i < workerThreadCount; i++) {
            final int workerIndex = i;
            Thread worker = new Thread(() -> drainQueue(workerIndex), "nr-emit-worker-" + i);
            worker.setDaemon(true);
            worker.start();
        }
        log.info("NR emit service started: workers={} queueCapacity={}", workerThreadCount, queueCapacity);
    }

    /**
     * Continuously drains {@link #eventQueue}, calling {@code sendBatch} for each entry.
     * Multiple instances run concurrently — the queue is thread-safe and distributes
     * work across all workers automatically.
     * Individual send failures are logged and swallowed; the loop continues.
     */
    private void drainQueue(int workerIndex) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                EventBatch batch = eventQueue.poll(1, TimeUnit.SECONDS);
                if (batch == null) {
                    continue;
                }
                try {
                    eventBatchSender.sendBatch(batch);
                } catch (Exception e) {
                    log.warn("NR worker-{} sendBatch failed: {}", workerIndex, e.getMessage());
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // -----------------------------------------------------------------------
    // Public event emitters
    // -----------------------------------------------------------------------

    /**
     * Emits a {@code MessageTrackingResult} event representing the final outcome of a
     * fully-tracked message.
     *
     * @param summary               the finalized summary row
     * @param storeFinalizationTimes map of storeNumber -&gt; finalization timestamp
     */
    public void emitMessageTrackingResult(MtsSummary summary,
                                           Map<String, Instant> storeFinalizationTimes) {
        try {
            Map<String, Object> attrs = new LinkedHashMap<>();
            attrs.put("messageKey", summary.getMessageKey());
            attrs.put("clusterId", summary.getClusterId());
            attrs.put("msgOffset", summary.getMsgOffset());
            attrs.put("state", summary.getState());
            attrs.put("totalStores", summary.getTotalStores());
            attrs.put("storesDone", summary.getStoresDone());
            attrs.put("storesPartial", summary.getStoresPartial());
            attrs.put("storesTimedOut", summary.getStoresTimedOut());
            attrs.put("publishCount", summary.getPublishCount());
            attrs.put("inconsistencyCount", summary.getInconsistencies().size());
            attrs.put("inconsistencies", String.join(",", summary.getInconsistencies()));
            attrs.put("firstPublishedAt", toEpochMillis(summary.getFirstPublishedAt()));
            attrs.put("lastPublishedAt", toEpochMillis(summary.getLastPublishedAt()));
            attrs.put("expireAt", toEpochMillis(summary.getExpireAt()));
            attrs.put("trackingDurationMs", computeTrackingDuration(summary));

            if (storeFinalizationTimes != null && !storeFinalizationTimes.isEmpty()) {
                attrs.put("storeFinalizationCount", storeFinalizationTimes.size());
            }

            postEvents(List.of(attrs), "MessageTrackingResult");

        } catch (Exception e) {
            log.warn("Failed to emit MessageTrackingResult for messageKey={}: {}",
                    summary.getMessageKey(), e.getMessage());
        }
    }

    /**
     * Emits a {@code StoreTrackingResult} event representing the state of a store at a
     * particular completion checkpoint.
     *
     * @param store         the store row (may be mid-tracking or final)
     * @param checkpointPct the completion percentage that triggered this event
     */
    public void emitStoreTrackingResult(MtsStore store, int checkpointPct) {
        try {
            int expected  = store.getExpectedPos()  == null ? 0 : store.getExpectedPos().size();
            int responded = store.getRespondedPos() == null ? 0 : store.getRespondedPos().size();

            Map<String, Object> attrs = new LinkedHashMap<>();
            attrs.put("messageKey", store.getMessageKey());
            attrs.put("clusterId", store.getClusterId());
            attrs.put("storeNumber", store.getStoreNumber());
            attrs.put("locationId", store.getLocationId());
            attrs.put("state", store.getState());
            attrs.put("checkpointPct", checkpointPct);
            attrs.put("expectedPosCount", expected);
            attrs.put("respondedPosCount", responded);
            attrs.put("missingPosCount", store.getMissingPos() == null ? 0 : store.getMissingPos().size());
            attrs.put("inconsistencyCount", store.getInconsistencies() == null ? 0 : store.getInconsistencies().size());
            attrs.put("inconsistencies", store.getInconsistencies() == null ? ""
                    : String.join(",", store.getInconsistencies()));
            attrs.put("expireAt", toEpochMillis(store.getExpireAt()));

            postEvents(List.of(attrs), "StoreTrackingResult");

        } catch (Exception e) {
            log.warn("Failed to emit StoreTrackingResult for messageKey={} store={}: {}",
                    store.getMessageKey(), store.getStoreNumber(), e.getMessage());
        }
    }

    /**
     * Emits one {@code PosTrackingResult} event per problematic POS machine
     * (MISSING, FAILED, DELIVERED_NO_ACK).
     *
     * Events are batched into groups of {@value #NR_BATCH_LIMIT} per send call.
     *
     * @param store            the finalized store row
     * @param firstPublishedAt timestamp of the original publish (used to compute lag)
     */
    public void emitPosTrackingResults(MtsStore store, Instant firstPublishedAt) {
        try {
            List<Map<String, Object>> events = new ArrayList<>();

            if (store.getMissingPos() != null) {
                for (String posHost : store.getMissingPos()) {
                    events.add(buildPosAttrs(store, posHost, STATUS_MISSING, null, firstPublishedAt));
                }
            }

            if (store.getPosStatuses() != null) {
                for (Map.Entry<String, PosStatus> entry : store.getPosStatuses().entrySet()) {
                    PosStatus ps = entry.getValue();
                    if (ps.getStatus() == null) continue;
                    boolean isProblem = STATUS_FAILED.equals(ps.getStatus())
                            || STATUS_DELIVERED_NO_ACK.equals(ps.getStatus());
                    if (isProblem) {
                        events.add(buildPosAttrs(store, entry.getKey(), ps.getStatus(),
                                ps.getPatchReceivedAt(), firstPublishedAt));
                    }
                }
            }

            if (events.isEmpty()) return;

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
     * Emits a {@code MessageTrackingResult} event with state=REPLACED for the old summary
     * that is being superseded by a re-publish.
     *
     * @param oldSummary the summary row that will be replaced
     */
    public void emitReplacedEvent(MtsSummary oldSummary) {
        try {
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

    private Map<String, Object> buildPosAttrs(MtsStore store,
                                               String posHost,
                                               String status,
                                               Instant patchReceivedAt,
                                               Instant firstPublishedAt) {
        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("messageKey", store.getMessageKey());
        attrs.put("clusterId", store.getClusterId());
        attrs.put("storeNumber", store.getStoreNumber());
        attrs.put("locationId", store.getLocationId());
        attrs.put("posHostname", posHost);
        attrs.put("status", status);
        attrs.put("expireAt", toEpochMillis(store.getExpireAt()));
        attrs.put("firstPublishedAt", toEpochMillis(firstPublishedAt));

        if (patchReceivedAt != null && firstPublishedAt != null) {
            attrs.put("responselagMs", ChronoUnit.MILLIS.between(firstPublishedAt, patchReceivedAt));
        }

        return attrs;
    }

    /**
     * Converts attribute maps to NR SDK {@link Event} objects and enqueues the batch
     * for async dispatch by the worker thread.  This method is non-blocking: if the
     * queue is full the batch is dropped with a warning rather than blocking the caller.
     * All exceptions are swallowed — telemetry must never disrupt core tracking logic.
     */
    private void postEvents(List<Map<String, Object>> attrMaps, String eventType) {
        if (attrMaps == null || attrMaps.isEmpty()) return;
        try {
            long now = System.currentTimeMillis();
            List<Event> events = new ArrayList<>(attrMaps.size());
            for (Map<String, Object> map : attrMaps) {
                events.add(new Event(eventType, toAttributes(map), now));
            }
            EventBatch batch = new EventBatch(events, new Attributes());
            boolean offered = eventQueue.offer(batch, 100, TimeUnit.MILLISECONDS);
            if (!offered) {
                log.error("NR event queue full — dropping {} {} events", events.size(), eventType);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("NR queue offer interrupted for eventType={}", eventType);
        } catch (Exception e) {
            log.warn("NR queue offer failed for eventType={}: {}", eventType, e.getMessage());
        }
    }

    /**
     * Converts a plain {@code Map<String, Object>} to an NR {@link Attributes} instance.
     * Null values are skipped. Unrecognised types fall back to {@code toString()}.
     */
    private Attributes toAttributes(Map<String, Object> map) {
        Attributes attrs = new Attributes();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object v = entry.getValue();
            if (v == null) continue;
            String k = entry.getKey();
            if      (v instanceof String)  { attrs.put(k, (String)  v); }
            else if (v instanceof Long)    { attrs.put(k, (Long)    v); }
            else if (v instanceof Integer) { attrs.put(k, (long)(int)(Integer) v); }
            else if (v instanceof Double)  { attrs.put(k, (Double)  v); }
            else if (v instanceof Boolean) { attrs.put(k, (Boolean) v); }
            else                           { attrs.put(k, v.toString()); }
        }
        return attrs;
    }

    private static long toEpochMillis(Instant instant) {
        return instant == null ? 0L : instant.toEpochMilli();
    }

    private static long computeTrackingDuration(MtsSummary summary) {
        if (summary.getFirstPublishedAt() == null) return 0L;
        Instant end = summary.getUpdatedAt() != null ? summary.getUpdatedAt() : Instant.now();
        return ChronoUnit.MILLIS.between(summary.getFirstPublishedAt(), end);
    }
}
