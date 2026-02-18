package com.pos.inconsistency.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;

/**
 * Response body for {@code GET /api/message-tracking/{messageKey}/status}.
 *
 * Provides a complete snapshot of message-level and store-level tracking state.
 */
public record StatusResponse(

        /**
         * Business message identifier.
         */
        @JsonProperty("messageKey")
        String messageKey,

        /**
         * Kafka cluster identifier.
         */
        @JsonProperty("clusterId")
        String clusterId,

        /**
         * Current lifecycle state of the message summary.
         * One of: PENDING, DONE, DEGRADED, REPLACED.
         */
        @JsonProperty("state")
        String state,

        /**
         * Percentage of stores that have been fully finalized (DONE or TIMED_OUT),
         * expressed as a value between 0.0 and 100.0.
         */
        @JsonProperty("clusterCompletionPct")
        double clusterCompletionPct,

        /**
         * Total number of stores registered for this message.
         */
        @JsonProperty("totalStores")
        int totalStores,

        /**
         * Number of stores that completed successfully.
         */
        @JsonProperty("storesDone")
        int storesDone,

        /**
         * Number of stores that completed with partial POS responses.
         */
        @JsonProperty("storesPartial")
        int storesPartial,

        /**
         * Number of stores that are still waiting (neither done nor timed-out).
         */
        @JsonProperty("storesPending")
        int storesPending,

        /**
         * Number of stores that timed out without full POS responses.
         */
        @JsonProperty("storesTimedOut")
        int storesTimedOut,

        /**
         * Timestamp of the first publish event for this messageKey.
         */
        @JsonProperty("firstPublishedAt")
        Instant firstPublishedAt,

        /**
         * Absolute expiry timestamp for the tracking window.
         */
        @JsonProperty("expireAt")
        Instant expireAt,

        /**
         * Message-level inconsistency type names.
         */
        @JsonProperty("inconsistencies")
        List<String> inconsistencies,

        /**
         * Per-store tracking details.
         */
        @JsonProperty("stores")
        List<StoreStatusDetail> stores

) {
}
