package com.pos.inconsistency.service;

import com.pos.inconsistency.model.InconsistencyType;
import com.pos.inconsistency.model.MtsStore;
import com.pos.inconsistency.model.MtsSummary;
import com.pos.inconsistency.model.PosStatus;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Stateless service that analyses a fully-loaded {@link MtsStore} or
 * {@link MtsSummary} and returns all applicable {@link InconsistencyType}s.
 *
 * This service intentionally has no side-effects — it does not write to the
 * database and does not emit any telemetry.  Callers are responsible for
 * persisting detected inconsistencies and triggering downstream events.
 *
 * Detection logic is deliberately kept simple and deterministic so that it
 * can be unit-tested without any database or network dependency.
 */
@Singleton
public class InconsistencyDetectionService {

    private static final Logger log = LoggerFactory.getLogger(InconsistencyDetectionService.class);

    // Status constants shared with the POS domain
    private static final String STATUS_DONE             = "DONE";
    private static final String STATUS_DELIVERED        = "DELIVERED";
    private static final String STATUS_DELIVERED_NO_ACK = "DELIVERED_NO_ACK";
    private static final String STATUS_FAILED           = "FAILED";

    // -----------------------------------------------------------------------
    // Store-level detection
    // -----------------------------------------------------------------------

    /**
     * Detects all inconsistencies applicable to a single store tracking row.
     *
     * <p>Checks performed:
     * <ol>
     *   <li><b>CONFLICTING_STATUS</b> – the posStatuses map contains at least one POS
     *       with DONE/DELIVERED and at least one with DELIVERED_NO_ACK, which indicates
     *       contradictory delivery outcomes within the same store.</li>
     *   <li><b>MISSING_POS</b> – one or more POS machines that were expected to respond
     *       have not yet done so (missingPos is non-empty after the tracking window closed).</li>
     *   <li><b>CONSUMER_ACK_MISSING</b> – one or more POS machines have a status of DONE or
     *       DELIVERED but carry no consumer ACK timestamp, which means the message was
     *       processed but the consumer did not confirm receipt.</li>
     * </ol>
     *
     * <p>Checks <em>not</em> performed here:
     * <ul>
     *   <li>LATE_POS_RESPONSE – checked inline during PATCH processing in
     *       {@link MessageTrackingService#handlePosResponse}.</li>
     *   <li>DUPLICATE_KEY / OFFSET_MISMATCH – checked at publish time in
     *       {@link MessageTrackingService#handlePublish}.</li>
     *   <li>PREMATURE_REPUBLISH – checked at publish time.</li>
     * </ul>
     *
     * @param store                    the finalized or expiring store row
     * @param firstPublishedAt         timestamp of the original publish (unused here,
     *                                 reserved for future LAG checks at finalize time)
     * @param latePosThresholdMinutes  late-response threshold in minutes (unused here;
     *                                 late checks happen in handlePosResponse)
     * @return ordered list of detected inconsistency types (may be empty)
     */
    public List<InconsistencyType> detectForStore(MtsStore store,
                                                   Instant firstPublishedAt,
                                                   int latePosThresholdMinutes) {
        List<InconsistencyType> detected = new ArrayList<>();

        Map<String, PosStatus> statuses = store.getPosStatuses();
        if (statuses == null) {
            statuses = Map.of();
        }

        // ---- 1. CONFLICTING_STATUS ----------------------------------------
        // Triggered when the same store has machines that are "fully done"
        // alongside machines that delivered but never got an ACK.
        boolean hasConfirmedDone = false;
        boolean hasDeliveredNoAck = false;

        for (PosStatus ps : statuses.values()) {
            if (ps.getStatus() == null) {
                continue;
            }
            if (STATUS_DONE.equals(ps.getStatus()) || STATUS_DELIVERED.equals(ps.getStatus())) {
                hasConfirmedDone = true;
            }
            if (STATUS_DELIVERED_NO_ACK.equals(ps.getStatus())) {
                hasDeliveredNoAck = true;
            }
        }

        if (hasConfirmedDone && hasDeliveredNoAck) {
            log.info("CONFLICTING_STATUS detected for messageKey={} store={}",
                    store.getMessageKey(), store.getStoreNumber());
            detected.add(InconsistencyType.CONFLICTING_STATUS);
        }

        // ---- 2. MISSING_POS ------------------------------------------------
        // Any POS machine that was expected but did not respond by finalization time.
        if (store.getMissingPos() != null && !store.getMissingPos().isEmpty()) {
            log.info("MISSING_POS detected for messageKey={} store={} missing={}",
                    store.getMessageKey(), store.getStoreNumber(), store.getMissingPos());
            detected.add(InconsistencyType.MISSING_POS);
        }

        // ---- 3. CONSUMER_ACK_MISSING ----------------------------------------
        // Any POS that is DONE or DELIVERED but has no consumer ACK timestamp.
        boolean acksAllPresent = true;
        for (Map.Entry<String, PosStatus> entry : statuses.entrySet()) {
            PosStatus ps = entry.getValue();
            if (ps.getStatus() == null) {
                continue;
            }
            boolean isDoneOrDelivered = STATUS_DONE.equals(ps.getStatus())
                    || STATUS_DELIVERED.equals(ps.getStatus());
            if (isDoneOrDelivered && ps.getConsumerAckTimestamp() == null) {
                log.info("CONSUMER_ACK_MISSING for pos={} messageKey={} store={}",
                        entry.getKey(), store.getMessageKey(), store.getStoreNumber());
                acksAllPresent = false;
                break; // one detection is enough — we flag at the store level
            }
        }
        if (!acksAllPresent) {
            detected.add(InconsistencyType.CONSUMER_ACK_MISSING);
        }

        return detected;
    }

    // -----------------------------------------------------------------------
    // Message-level detection
    // -----------------------------------------------------------------------

    /**
     * Detects inconsistencies that are only visible at the message (summary) level,
     * after individual store finalization is complete.
     *
     * <p>Checks performed:
     * <ol>
     *   <li><b>MISSING_STORE</b> – one or more stores timed out, meaning they had
     *       no (or insufficient) POS responses before the tracking window closed.</li>
     * </ol>
     *
     * @param summary the fully-finalized (or being-finalized) summary row
     * @return ordered list of detected message-level inconsistency types (may be empty)
     */
    public List<InconsistencyType> detectForMessage(MtsSummary summary) {
        List<InconsistencyType> detected = new ArrayList<>();

        // MISSING_STORE: any timed-out store implies the store had zero (or insufficient)
        // POS responses before expiry.
        if (summary.getStoresTimedOut() > 0) {
            log.info("MISSING_STORE detected for messageKey={} storesTimedOut={}",
                    summary.getMessageKey(), summary.getStoresTimedOut());
            detected.add(InconsistencyType.MISSING_STORE);
        }

        return detected;
    }
}
