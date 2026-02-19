package com.pos.inconsistency.service;

import com.pos.inconsistency.model.*;
import com.pos.inconsistency.repository.MtsStoreRepository;
import com.pos.inconsistency.repository.MtsSummaryRepository;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.http.HttpStatus;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Core orchestration service for message and store tracking.
 *
 * Responsibilities:
 * <ul>
 *   <li>Accept new publish events and initialise tracking rows</li>
 *   <li>Handle POS response PATCH events and advance store state</li>
 *   <li>Finalise stores when all POS machines have responded or expired</li>
 *   <li>Finalise messages when all stores have been finalised</li>
 *   <li>Expose a status query for the REST API</li>
 * </ul>
 *
 * The SELECT FOR UPDATE pattern in {@link #handlePosResponse} is implemented by
 * manually managing a JDBC connection so that the lock spans the read-and-update
 * within a single PostgreSQL transaction.  Micronaut's {@code @Transactional}
 * is deliberately not used here because JDBC-level transaction management gives
 * full visibility and avoids hidden commit/rollback interactions with Hikari.
 */
@Singleton
public class MessageTrackingService {

    private static final Logger log = LoggerFactory.getLogger(MessageTrackingService.class);

    // State constants
    private static final String STATE_PENDING    = "PENDING";
    private static final String STATE_PARTIAL    = "PARTIAL";
    private static final String STATE_DONE       = "DONE";
    private static final String STATE_FINALIZING = "FINALIZING";
    private static final String STATE_TIMED_OUT  = "TIMED_OUT";
    private static final String STATE_DEGRADED   = "DEGRADED";
    private static final String STATE_REPLACED   = "REPLACED";

    // POS status constants (mirrored from domain)
    private static final String STATUS_DONE             = "DONE";
    private static final String STATUS_DELIVERED        = "DELIVERED";
    private static final String STATUS_DELIVERED_NO_ACK = "DELIVERED_NO_ACK";
    private static final String STATUS_FAILED           = "FAILED";
    private static final String STATUS_MISSING          = "MISSING";

    @Inject
    private MtsSummaryRepository summaryRepo;

    @Inject
    private MtsStoreRepository storeRepo;

    @Inject
    private InconsistencyDetectionService detectionService;

    @Inject
    private NewRelicEmitService nrEmitService;

    @Value("${inconsistency.late-pos-threshold-minutes:60}")
    private int latePosThresholdMinutes;

    @Value("${inconsistency.checkpoint-thresholds:1,50,100}")
    private String checkpointThresholdsConfig;

    @Value("${inconsistency.finalizing-stale-minutes:5}")
    private int finalizingStaleMinutes;

    // Parsed checkpoint percentages; populated lazily
    private volatile int[] checkpointThresholds;

    // -----------------------------------------------------------------------
    // Publish handling
    // -----------------------------------------------------------------------

    /**
     * Processes an inbound publish event.
     *
     * <p>Two scenarios:
     * <ol>
     *   <li><b>New message</b>: Insert a summary row and bulk-insert one store row per store.</li>
     *   <li><b>Re-publish (replace)</b>: Emit a REPLACED event for the old tracking row,
     *       delete the old store rows and summary row, then insert fresh rows.
     *       Flags DUPLICATE_KEY always; also OFFSET_MISMATCH when the Kafka offset changes.</li>
     * </ol>
     *
     * @param event the inbound publish event (already validated by Micronaut)
     */
    public void handlePublish(PublishEvent event) {
        log.info("handlePublish messageKey={} clusterId={} stores={}",
                event.messageKey(), event.clusterId(), event.stores().size());

        Optional<MtsSummary> existing = summaryRepo.findByMessageKey(event.messageKey());

        List<String> inconsistencies = new ArrayList<>();
        Instant now = Instant.now();

        if (existing.isPresent()) {
            MtsSummary old = existing.get();
            log.warn("Re-publish detected for messageKey={} previousOffset={} newOffset={}",
                    event.messageKey(), old.getMsgOffset(), event.msgOffset());

            // Always flag a duplicate key on re-publish
            inconsistencies.add(InconsistencyType.DUPLICATE_KEY.name());

            // Premature re-publish: previous tracking window still open
            if (old.getExpireAt() != null && old.getExpireAt().isAfter(now)) {
                log.warn("PREMATURE_REPUBLISH for messageKey={} — previous window still open until {}",
                        event.messageKey(), old.getExpireAt());
                inconsistencies.add(InconsistencyType.PREMATURE_REPUBLISH.name());
            }

            // Offset mismatch if the Kafka offset changed
            if (old.getMsgOffset() != null && !old.getMsgOffset().equals(event.msgOffset())) {
                log.warn("OFFSET_MISMATCH for messageKey={} old={} new={}",
                        event.messageKey(), old.getMsgOffset(), event.msgOffset());
                inconsistencies.add(InconsistencyType.OFFSET_MISMATCH.name());
            }

        }

        // Build the new summary row
        MtsSummary summary = MtsSummary.builder()
                .messageKey(event.messageKey())
                .clusterId(event.clusterId())
                .msgOffset(event.msgOffset())
                .firstPublishedAt(existing.map(MtsSummary::getFirstPublishedAt).orElse(now))
                .lastPublishedAt(now)
                .expireAt(event.expireAt())
                .totalStores(event.stores().size())
                .storesDone(0)
                .storesPartial(0)
                .storesTimedOut(0)
                .publishCount(existing.map(s -> s.getPublishCount() + 1).orElse(1))
                .state(STATE_PENDING)
                .inconsistencies(inconsistencies)
                .build();

        // Build store rows
        List<MtsStore> storeRows = new ArrayList<>();
        for (StoreDetail detail : event.stores()) {
            List<String> expectedPos = detail.expectedPosHosts() == null
                    ? new ArrayList<>() : new ArrayList<>(detail.expectedPosHosts());

            storeRows.add(MtsStore.builder()
                    .messageKey(event.messageKey())
                    .clusterId(event.clusterId())
                    .storeNumber(detail.storeNumber())
                    .locationId(detail.locationId())
                    .expireAt(event.expireAt())
                    .expectedPos(expectedPos)
                    .respondedPos(new ArrayList<>())
                    .missingPos(new ArrayList<>(expectedPos))
                    .posStatuses(new HashMap<>())
                    .state(STATE_PENDING)
                    .inconsistencies(new ArrayList<>())
                    .lastCheckpointPct(0)
                    .build());
        }

        // Persist summary + store rows in a single transaction to avoid partial writes
        try (Connection conn = storeRepo.getDataSource().getConnection()) {
            conn.setAutoCommit(false);
            try {
                if (existing.isPresent()) {
                    storeRepo.deleteAllByMessageKey(conn, event.messageKey());
                    summaryRepo.delete(conn, event.messageKey());
                }
                summaryRepo.save(conn, summary);
                storeRepo.saveAll(conn, storeRows);
                conn.commit();

                // Emit REPLACED only after the replacement is durably committed.
                // Emitting before commit would send the event even if the transaction
                // rolls back, creating a phantom REPLACED record in New Relic.
                if (existing.isPresent()) {
                    nrEmitService.emitReplacedEvent(existing.get());
                }
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            log.error("DB connection error in handlePublish messageKey={}", event.messageKey(), e);
            throw new RuntimeException("DB connection error in handlePublish", e);
        }

        log.info("Publish handled messageKey={} totalStores={} inconsistencies={}",
                event.messageKey(), storeRows.size(), inconsistencies);
    }

    // -----------------------------------------------------------------------
    // POS response handling
    // -----------------------------------------------------------------------

    /**
     * Processes a POS response PATCH event.
     *
     * The implementation:
     * <ol>
     *   <li>Quick read (no lock) to check the store exists — avoids acquiring a lock
     *       on stale or unknown events.</li>
     *   <li>Opens a JDBC transaction and issues SELECT FOR UPDATE to serialize
     *       concurrent responses for the same store.</li>
     *   <li>Updates posStatuses, moves the host from missingPos to respondedPos.</li>
     *   <li>Checks for a late response and flags LATE_POS_RESPONSE if applicable.</li>
     *   <li>Advances store state and emits checkpoint events.</li>
     *   <li>If the store is now fully done (all POS responded), calls {@link #finalizeStore}.</li>
     * </ol>
     *
     * @param messageKey the message identifier from the URL path
     * @param event      the POS response event body
     */
    public void handlePosResponse(String messageKey, PosResponseEvent event) {
        log.info("handlePosResponse messageKey={} store={} pos={} status={}",
                messageKey, event.storeNumber(), event.posHostname(), event.status());

        // Quick read — if not found, this is a stale/unknown event
        Optional<MtsStore> quickRead = storeRepo.findByMessageKeyAndStore(messageKey, event.storeNumber());
        if (quickRead.isEmpty()) {
            log.warn("Stale POS response ignored — no active tracking for messageKey={} store={} pos={}",
                    messageKey, event.storeNumber(), event.posHostname());
            return;
        }

        // Open a manual JDBC transaction to hold the FOR UPDATE lock
        try (Connection conn = storeRepo.getDataSource().getConnection()) {
            conn.setAutoCommit(false);
            try {
                Optional<MtsStore> lockedOpt = storeRepo.findByMessageKeyAndStoreForUpdate(
                        conn, messageKey, event.storeNumber());

                if (lockedOpt.isEmpty()) {
                    // Row was deleted between the quick read and the FOR UPDATE — race condition
                    log.warn("Store row disappeared between quick-read and FOR UPDATE messageKey={} store={}",
                            messageKey, event.storeNumber());
                    conn.rollback();
                    return;
                }

                MtsStore store = lockedOpt.get();

                // Reject responses for rows that are already in a terminal or in-progress
                // finalization state.  FINALIZING means claimForFinalization already wrote
                // the claim; DONE means all POS responded and finalization is about to run;
                // TIMED_OUT means the cleanup path already finalized the row.
                // Mutating these rows would corrupt data that finalization has already "frozen".
                if (STATE_FINALIZING.equals(store.getState())
                        || STATE_DONE.equals(store.getState())
                        || STATE_TIMED_OUT.equals(store.getState())) {
                    log.info("Late POS response ignored — store is in terminal state {} messageKey={} store={} pos={}",
                            store.getState(), messageKey, event.storeNumber(), event.posHostname());
                    conn.rollback();
                    return;
                }

                // Fetch the summary to get firstPublishedAt (needed for lag check)
                Optional<MtsSummary> summaryOpt = summaryRepo.findByMessageKey(messageKey);
                Instant firstPublishedAt = summaryOpt
                        .map(MtsSummary::getFirstPublishedAt)
                        .orElse(store.getCreatedAt());

                Instant patchReceivedAt = Instant.now();

                // Parse consumerAckTimestamp from string (may be null)
                Instant consumerAckInstant = parseInstant(event.consumerAckTimestamp());

                // Build the updated PosStatus entry
                PosStatus posStatus = new PosStatus(
                        event.status(),
                        consumerAckInstant,
                        patchReceivedAt
                );

                // Update posStatuses map
                if (store.getPosStatuses() == null) {
                    store.setPosStatuses(new HashMap<>());
                }
                store.getPosStatuses().put(event.posHostname(), posStatus);

                // Move hostname from missingPos to respondedPos
                boolean wasInMissing = false;
                if (store.getMissingPos() != null) {
                    wasInMissing = store.getMissingPos().remove(event.posHostname());
                }
                if (wasInMissing) {
                    if (store.getRespondedPos() == null) {
                        store.setRespondedPos(new ArrayList<>());
                    }
                    store.getRespondedPos().add(event.posHostname());
                } else {
                    log.warn("POS hostname {} not in missingPos for messageKey={} store={} — possible duplicate response",
                            event.posHostname(), messageKey, event.storeNumber());
                }

                // ---- Late POS Response check --------------------------------
                long lagMinutes = ChronoUnit.MINUTES.between(firstPublishedAt, patchReceivedAt);
                if (lagMinutes > latePosThresholdMinutes) {
                    log.warn("LATE_POS_RESPONSE for pos={} messageKey={} store={} lagMin={}",
                            event.posHostname(), messageKey, event.storeNumber(), lagMinutes);
                    if (store.getInconsistencies() == null) {
                        store.setInconsistencies(new ArrayList<>());
                    }
                    String latePosFlag = InconsistencyType.LATE_POS_RESPONSE.name();
                    if (!store.getInconsistencies().contains(latePosFlag)) {
                        store.getInconsistencies().add(latePosFlag);
                    }
                }

                // ---- Compute POS completion percentage ----------------------
                int expectedCount = store.getExpectedPos() == null ? 0 : store.getExpectedPos().size();
                int respondedCount = store.getRespondedPos() == null ? 0 : store.getRespondedPos().size();
                int posCompletionPct = (expectedCount == 0)
                        ? 100
                        : (int) Math.round((respondedCount * 100.0) / expectedCount);

                // ---- Advance store state ------------------------------------
                if (respondedCount >= expectedCount) {
                    store.setState(STATE_DONE);
                } else if (respondedCount > 0) {
                    store.setState(STATE_PARTIAL);
                }
                // otherwise remains PENDING

                // ---- Checkpoint emission ------------------------------------
                int[] thresholds = getCheckpointThresholds();
                int previousCheckpoint = store.getLastCheckpointPct();
                int newCheckpoint = previousCheckpoint;

                for (int threshold : thresholds) {
                    if (posCompletionPct >= threshold && threshold > previousCheckpoint) {
                        newCheckpoint = threshold;
                    }
                }

                if (newCheckpoint > previousCheckpoint) {
                    store.setLastCheckpointPct(newCheckpoint);
                    // Persist before emitting NR event so the checkpoint is durable
                    storeRepo.update(conn, store);
                    conn.commit();

                    // NR emission is fire-and-forget — outside the transaction
                    final int emitCheckpoint = newCheckpoint;
                    final MtsStore emitStore = copyStore(store);
                    nrEmitService.emitStoreTrackingResult(emitStore, emitCheckpoint);
                } else {
                    storeRepo.update(conn, store);
                    conn.commit();
                }

                // ---- Finalize if all POS responded -------------------------
                if (STATE_DONE.equals(store.getState())) {
                    finalizeStore(store);
                }

            } catch (Exception e) {
                try {
                    conn.rollback();
                } catch (SQLException rollbackEx) {
                    log.error("Rollback failed for messageKey={} store={}", messageKey, event.storeNumber(), rollbackEx);
                }
                log.error("Error in handlePosResponse messageKey={} store={} pos={}",
                        messageKey, event.storeNumber(), event.posHostname(), e);
                throw new RuntimeException("Error handling POS response", e);
            }
        } catch (SQLException e) {
            log.error("DB connection error in handlePosResponse messageKey={}", messageKey, e);
            throw new RuntimeException("DB connection error in handlePosResponse", e);
        }
    }

    // -----------------------------------------------------------------------
    // Store finalization
    // -----------------------------------------------------------------------

    /**
     * Finalizes a store tracking row.
     *
     * Steps:
     * <ol>
     *   <li>Run inconsistency detection for the store.</li>
     *   <li>Set final state (DONE or TIMED_OUT) and merge inconsistencies.</li>
     *   <li>Emit final StoreTrackingResult and PosTrackingResult events to NR.</li>
     *   <li>Increment the appropriate counter on the summary.</li>
     *   <li>Delete the store row.</li>
     *   <li>Check if all stores for the message are now finalized — if so, finalize the message.</li>
     * </ol>
     *
     * @param store the store to finalize (state may be DONE or PARTIAL/PENDING for expired rows)
     */
    public void finalizeStore(MtsStore store) {
        log.info("finalizeStore messageKey={} store={} currentState={}",
                store.getMessageKey(), store.getStoreNumber(), store.getState());

        // ---- Idempotency guard -------------------------------------------------
        // Atomically claim the row by moving it to FINALIZING.  If another thread
        // (handlePosResponse or the cleanup scheduler) already claimed it, we get
        // empty back and must return immediately to avoid double-counting counters
        // or emitting duplicate NR events.
        Optional<MtsStore> claimed = storeRepo.claimForFinalization(
                store.getMessageKey(), store.getStoreNumber());

        if (claimed.isEmpty()) {
            log.info("finalizeStore skipped — row already claimed or deleted messageKey={} store={}",
                    store.getMessageKey(), store.getStoreNumber());
            return;
        }

        // Use the freshly claimed row (current DB state, not the stale caller snapshot)
        MtsStore current = claimed.get();

        // Fetch summary for firstPublishedAt
        Optional<MtsSummary> summaryOpt = summaryRepo.findByMessageKey(current.getMessageKey());
        Instant firstPublishedAt = summaryOpt
                .map(MtsSummary::getFirstPublishedAt)
                .orElse(current.getCreatedAt());

        // Detect store-level inconsistencies
        List<InconsistencyType> detected = detectionService.detectForStore(
                current, firstPublishedAt, latePosThresholdMinutes);

        // Merge newly detected inconsistencies (don't add duplicates)
        if (current.getInconsistencies() == null) {
            current.setInconsistencies(new ArrayList<>());
        }
        for (InconsistencyType type : detected) {
            String typeName = type.name();
            if (!current.getInconsistencies().contains(typeName)) {
                current.setInconsistencies(new ArrayList<>(current.getInconsistencies()));
                current.getInconsistencies().add(typeName);
            }
        }

        // Determine final state.
        // A store is timed-out when it expired with POS machines still missing.
        // This avoids depending on the pre-claim state value (which is now FINALIZING).
        boolean hasMissingPos = current.getMissingPos() != null && !current.getMissingPos().isEmpty();
        boolean expired = current.getExpireAt() != null && Instant.now().isAfter(current.getExpireAt());
        boolean timedOut = expired && hasMissingPos;

        String finalState = timedOut ? STATE_TIMED_OUT : STATE_DONE;
        current.setState(finalState);

        log.info("Finalizing store messageKey={} store={} finalState={} inconsistencies={}",
                current.getMessageKey(), current.getStoreNumber(), finalState, current.getInconsistencies());

        // Update summary counters + delete store row in a single transaction
        try (Connection conn = storeRepo.getDataSource().getConnection()) {
            conn.setAutoCommit(false);
            try {
                if (STATE_TIMED_OUT.equals(finalState)) {
                    summaryRepo.incrementStoresTimedOut(conn, current.getMessageKey());
                } else {
                    if (!hasMissingPos) {
                        summaryRepo.incrementStoresDone(conn, current.getMessageKey());
                    } else {
                        summaryRepo.incrementStoresPartial(conn, current.getMessageKey());
                    }
                }
                storeRepo.delete(conn, current.getMessageKey(), current.getStoreNumber());
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            log.error("DB error in finalizeStore counters+delete messageKey={} store={}",
                    current.getMessageKey(), current.getStoreNumber(), e);
            throw new RuntimeException("DB error in finalizeStore counters+delete", e);
        }

        // ---- NR events ---------------------------------------------------------
        // Emit after commit so retries don't duplicate events.
        // Emit 100% checkpoint only if it was not already emitted by handlePosResponse.
        if (current.getLastCheckpointPct() < 100) {
            nrEmitService.emitStoreTrackingResult(current, 100);
        }
        nrEmitService.emitPosTrackingResults(current, firstPublishedAt);

        // Re-fetch summary to check if all stores are now finalized
        Optional<MtsSummary> refreshedSummaryOpt = summaryRepo.findByMessageKey(current.getMessageKey());
        if (refreshedSummaryOpt.isEmpty()) {
            log.info("Summary already gone for messageKey={} — skipping message finalization",
                    current.getMessageKey());
            return;
        }

        MtsSummary refreshedSummary = refreshedSummaryOpt.get();
        int finalized = refreshedSummary.getStoresDone()
                + refreshedSummary.getStoresPartial()
                + refreshedSummary.getStoresTimedOut();

        log.info("Store finalized messageKey={} finalized={} total={}",
                current.getMessageKey(), finalized, refreshedSummary.getTotalStores());

        if (finalized >= refreshedSummary.getTotalStores()) {
            finalizeMessage(refreshedSummary);
        }
    }

    // -----------------------------------------------------------------------
    // Message finalization
    // -----------------------------------------------------------------------

    /**
     * Finalizes the message-level summary once all stores have been finalized.
     *
     * Steps:
     * <ol>
     *   <li>Detect message-level inconsistencies.</li>
     *   <li>Merge with any existing summary-level inconsistencies.</li>
     *   <li>Set final state: DONE if no inconsistencies, DEGRADED otherwise.</li>
     *   <li>Emit a MessageTrackingResult event to NR.</li>
     *   <li>Delete the summary row.</li>
     * </ol>
     *
     * @param summary the summary to finalize
     */
    public void finalizeMessage(MtsSummary summary) {
        log.info("finalizeMessage messageKey={} totalStores={} done={} partial={} timedOut={}",
                summary.getMessageKey(), summary.getTotalStores(),
                summary.getStoresDone(), summary.getStoresPartial(), summary.getStoresTimedOut());

        Optional<MtsSummary> claimedOpt = summaryRepo.claimForFinalization(
                summary.getMessageKey(), finalizingStaleMinutes);
        if (claimedOpt.isEmpty()) {
            log.info("finalizeMessage skipped — already claimed or deleted messageKey={}",
                    summary.getMessageKey());
            return;
        }

        // Use the claimed row (current DB state)
        summary = claimedOpt.get();

        // Detect message-level inconsistencies
        List<InconsistencyType> detected = detectionService.detectForMessage(summary);

        if (summary.getInconsistencies() == null) {
            summary.setInconsistencies(new ArrayList<>());
        }

        List<String> allInconsistencies = new ArrayList<>(summary.getInconsistencies());
        for (InconsistencyType type : detected) {
            String typeName = type.name();
            if (!allInconsistencies.contains(typeName)) {
                allInconsistencies.add(typeName);
            }
        }
        summary.setInconsistencies(allInconsistencies);

        // Determine final state
        boolean hasInconsistencies = !summary.getInconsistencies().isEmpty()
                || summary.getStoresTimedOut() > 0
                || summary.getStoresPartial() > 0;

        String finalState = hasInconsistencies ? STATE_DEGRADED : STATE_DONE;
        summary.setState(finalState);
        summary.setUpdatedAt(Instant.now());

        log.info("Finalizing message messageKey={} finalState={} inconsistencies={}",
                summary.getMessageKey(), finalState, summary.getInconsistencies());

        // Emit NR event before deleting the summary row.
        // Ordering rationale:
        //   emit-then-delete: if delete fails, the row stays FINALIZING and the cleanup
        //     scheduler retries. On retry, the event may fire again (at-most-twice).
        //   delete-then-emit: if the NR queue is full on emit, the event is permanently
        //     lost because the row is already gone and no retry path exists.
        // Note: if the NR queue is full at emit time, the event can still be dropped
        // even with emit-then-delete (at-most-once under NR saturation).
        nrEmitService.emitMessageTrackingResult(summary, Map.of());

        // Delete the summary row
        summaryRepo.delete(summary.getMessageKey());
    }

    // -----------------------------------------------------------------------
    // Status API
    // -----------------------------------------------------------------------

    /**
     * Builds a complete status snapshot for the given messageKey.
     *
     * @param messageKey the message identifier to look up
     * @return a populated StatusResponse
     * @throws HttpStatusException with 404 if the messageKey is not found
     */
    public StatusResponse getStatus(String messageKey) {
        Optional<MtsSummary> summaryOpt = summaryRepo.findByMessageKey(messageKey);
        if (summaryOpt.isEmpty()) {
            throw new HttpStatusException(HttpStatus.NOT_FOUND,
                    "No active tracking found for messageKey=" + messageKey);
        }

        MtsSummary summary = summaryOpt.get();
        List<MtsStore> stores = storeRepo.findAllByMessageKey(messageKey);

        // Compute cluster completion percentage
        int finalized = summary.getStoresDone() + summary.getStoresPartial() + summary.getStoresTimedOut();
        double clusterPct = summary.getTotalStores() == 0
                ? 0.0
                : Math.round((finalized * 100.0 / summary.getTotalStores()) * 100.0) / 100.0;

        int storesPending = summary.getTotalStores() - finalized;

        // Build per-store details
        List<StoreStatusDetail> storeDetails = stores.stream()
                .map(this::buildStoreDetail)
                .collect(Collectors.toList());

        return new StatusResponse(
                summary.getMessageKey(),
                summary.getClusterId(),
                summary.getState(),
                clusterPct,
                summary.getTotalStores(),
                summary.getStoresDone(),
                summary.getStoresPartial(),
                storesPending,
                summary.getStoresTimedOut(),
                summary.getFirstPublishedAt(),
                summary.getExpireAt(),
                summary.getInconsistencies(),
                storeDetails
        );
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    private StoreStatusDetail buildStoreDetail(MtsStore store) {
        int expected = store.getExpectedPos() == null ? 0 : store.getExpectedPos().size();
        int responded = store.getRespondedPos() == null ? 0 : store.getRespondedPos().size();
        int missing = store.getMissingPos() == null ? 0 : store.getMissingPos().size();

        double posCompletionPct = (expected == 0)
                ? 100.0
                : Math.round((responded * 100.0 / expected) * 100.0) / 100.0;

        return new StoreStatusDetail(
                store.getStoreNumber(),
                store.getState(),
                posCompletionPct,
                responded,
                expected,
                missing,
                store.getMissingPos() == null ? List.of() : List.copyOf(store.getMissingPos()),
                store.getInconsistencies() == null ? List.of() : List.copyOf(store.getInconsistencies())
        );
    }

    private int[] getCheckpointThresholds() {
        if (checkpointThresholds != null) {
            return checkpointThresholds;
        }
        synchronized (this) {
            if (checkpointThresholds == null) {
                String[] parts = checkpointThresholdsConfig.split(",");
                int[] thresholds = new int[parts.length];
                for (int i = 0; i < parts.length; i++) {
                    thresholds[i] = Integer.parseInt(parts[i].trim());
                }
                Arrays.sort(thresholds);
                checkpointThresholds = thresholds;
            }
        }
        return checkpointThresholds;
    }

    private static Instant parseInstant(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (Exception e) {
            log.warn("Failed to parse consumerAckTimestamp '{}': {}", value, e.getMessage());
            return null;
        }
    }

    /**
     * Shallow-copies a store so that NR emission uses a stable snapshot
     * even if the original object is mutated before the async call completes.
     */
    private static MtsStore copyStore(MtsStore src) {
        return MtsStore.builder()
                .id(src.getId())
                .messageKey(src.getMessageKey())
                .clusterId(src.getClusterId())
                .storeNumber(src.getStoreNumber())
                .locationId(src.getLocationId())
                .expireAt(src.getExpireAt())
                .expectedPos(src.getExpectedPos() != null ? new ArrayList<>(src.getExpectedPos()) : new ArrayList<>())
                .respondedPos(src.getRespondedPos() != null ? new ArrayList<>(src.getRespondedPos()) : new ArrayList<>())
                .missingPos(src.getMissingPos() != null ? new ArrayList<>(src.getMissingPos()) : new ArrayList<>())
                .posStatuses(src.getPosStatuses() != null ? new HashMap<>(src.getPosStatuses()) : new HashMap<>())
                .state(src.getState())
                .inconsistencies(src.getInconsistencies() != null ? new ArrayList<>(src.getInconsistencies()) : new ArrayList<>())
                .lastCheckpointPct(src.getLastCheckpointPct())
                .createdAt(src.getCreatedAt())
                .updatedAt(src.getUpdatedAt())
                .build();
    }
}
