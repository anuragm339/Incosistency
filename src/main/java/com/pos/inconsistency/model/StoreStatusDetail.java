package com.pos.inconsistency.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Detail of a single store's tracking status, embedded within a {@link StatusResponse}.
 *
 * Provides a per-store view of POS completion and any detected inconsistencies.
 */
public record StoreStatusDetail(

        /**
         * Store identifier.
         */
        @JsonProperty("storeNumber")
        String storeNumber,

        /**
         * Current lifecycle state of the store row.
         * One of: PENDING, PARTIAL, DONE, TIMED_OUT.
         */
        @JsonProperty("state")
        String state,

        /**
         * Percentage of expected POS machines that have responded, rounded to two decimals.
         * Value is between 0.0 and 100.0.
         */
        @JsonProperty("posCompletionPct")
        double posCompletionPct,

        /**
         * Number of POS machines that have already responded.
         */
        @JsonProperty("respondedPosCount")
        int respondedPosCount,

        /**
         * Total number of POS machines that were expected to respond.
         */
        @JsonProperty("expectedPosCount")
        int expectedPosCount,

        /**
         * Number of POS machines that have not yet responded (or never responded).
         */
        @JsonProperty("missingPosCount")
        int missingPosCount,

        /**
         * Hostnames of POS machines that are still missing / timed-out.
         */
        @JsonProperty("missingPos")
        List<String> missingPos,

        /**
         * Inconsistency type names detected for this store.
         */
        @JsonProperty("inconsistencies")
        List<String> inconsistencies

) {
}
