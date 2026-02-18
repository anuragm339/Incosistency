package com.pos.inconsistency.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Plain Java bean representing a row in the {@code mts_summary} table.
 *
 * Intentionally kept free of ORM annotations — all persistence is handled
 * via plain JDBC in {@link com.pos.inconsistency.repository.MtsSummaryRepository}.
 *
 * Lifecycle states for {@code state}:
 * <ul>
 *   <li>PENDING  – message received, waiting for all stores to complete</li>
 *   <li>DONE     – all stores finalized without issues</li>
 *   <li>DEGRADED – finalized but with one or more inconsistencies</li>
 *   <li>REPLACED – superseded by a re-publish of the same messageKey</li>
 * </ul>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MtsSummary {

    /** Auto-generated surrogate key (BIGSERIAL from PostgreSQL). */
    private Long id;

    /** Business identifier that uniquely identifies the published message. */
    private String messageKey;

    /** Kafka cluster that originated the message. */
    private String clusterId;

    /** Kafka offset at which the message was published (may be null for legacy). */
    private Long msgOffset;

    /** Timestamp of the very first publish for this messageKey. */
    private Instant firstPublishedAt;

    /** Timestamp of the most recent publish (differs from first on re-publish). */
    private Instant lastPublishedAt;

    /** Absolute deadline: tracking expires after this time. */
    private Instant expireAt;

    /** Total number of stores that were expected to respond. */
    private int totalStores;

    /** Stores whose tracking completed successfully (state = DONE). */
    private int storesDone;

    /** Stores that completed with partial POS responses (state = PARTIAL). */
    private int storesPartial;

    /** Stores that did not respond before expiry (state = TIMED_OUT). */
    private int storesTimedOut;

    /** How many times this messageKey has been published (1 = first time). */
    private int publishCount;

    /**
     * Current lifecycle state of this summary row.
     * One of: PENDING, DONE, DEGRADED, REPLACED.
     */
    private String state;

    /**
     * Ordered list of detected {@link InconsistencyType} names.
     * Stored as a JSONB array in PostgreSQL.
     */
    @Builder.Default
    private List<String> inconsistencies = new ArrayList<>();

    /** Row creation timestamp (set by DB default on INSERT). */
    private Instant createdAt;

    /** Row last-modification timestamp (updated on every UPDATE). */
    private Instant updatedAt;
}
