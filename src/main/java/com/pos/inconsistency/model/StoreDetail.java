package com.pos.inconsistency.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Represents a single store entry within a {@link PublishEvent}.
 *
 * Each store carries the list of POS hostnames that are expected to respond
 * for the given message key.
 */
public record StoreDetail(

        /**
         * Store identifier (e.g. "0042", "NYC-TIMES-SQ").
         */
        @JsonProperty("storeNumber")
        String storeNumber,

        /**
         * Optional location or register identifier within the store.
         * May be null if the store does not differentiate by location.
         */
        @JsonProperty("locationId")
        String locationId,

        /**
         * Hostnames of all POS machines that are expected to send a PATCH response
         * for this message at this store.
         */
        @JsonProperty("expectedPosHosts")
        List<String> expectedPosHosts

) {
}
