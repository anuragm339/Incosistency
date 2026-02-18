package com.pos.inconsistency.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Plain Java bean representing a row in the {@code mts_store} table.
 *
 * One row exists per (messageKey, storeNumber) pair for the duration of the
 * tracking window. Rows are deleted once a store is finalized.
 *
 * Lifecycle states for {@code state}:
 * <ul>
 *   <li>PENDING   – store row created, no POS responses yet</li>
 *   <li>PARTIAL   – some POS machines responded, others still missing</li>
 *   <li>DONE      – all expected POS machines responded</li>
 *   <li>TIMED_OUT – tracking window expired before all POS machines responded</li>
 * </ul>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MtsStore {

    /** Auto-generated surrogate key (BIGSERIAL from PostgreSQL). */
    private Long id;

    /** Business message identifier, foreign-key style reference to mts_summary. */
    private String messageKey;

    /** Kafka cluster that originated the message. */
    private String clusterId;

    /** Store number (retail location identifier). */
    private String storeNumber;

    /** Optional physical location / register identifier within the store. */
    private String locationId;

    /** Absolute expiry deadline inherited from the summary. */
    private Instant expireAt;

    /**
     * Hostnames of POS machines that were expected to respond.
     * Stored as a JSONB string array.
     */
    @Builder.Default
    private List<String> expectedPos = new ArrayList<>();

    /**
     * Hostnames of POS machines that have already responded.
     * Moved from {@link #missingPos} when a PATCH event arrives.
     */
    @Builder.Default
    private List<String> respondedPos = new ArrayList<>();

    /**
     * Hostnames still waiting for a response.
     * Initialised as a copy of {@link #expectedPos} on INSERT.
     */
    @Builder.Default
    private List<String> missingPos = new ArrayList<>();

    /**
     * Keyed by POS hostname. Each value captures the current status and
     * timestamps for that machine.
     * Stored as a JSONB object ({@code Map<String, PosStatus>}).
     */
    @Builder.Default
    private Map<String, PosStatus> posStatuses = new HashMap<>();

    /**
     * Current lifecycle state.
     * One of: PENDING, PARTIAL, DONE, TIMED_OUT.
     */
    private String state;

    /**
     * Ordered list of detected {@link InconsistencyType} names for this store.
     * Stored as a JSONB array.
     */
    @Builder.Default
    private List<String> inconsistencies = new ArrayList<>();

    /**
     * Highest POS-completion checkpoint percentage that has already been emitted
     * (prevents duplicate NR events for the same milestone).
     */
    private int lastCheckpointPct;

    /** Row creation timestamp. */
    private Instant createdAt;

    /** Row last-modification timestamp. */
    private Instant updatedAt;
}
