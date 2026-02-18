package com.pos.inconsistency.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.List;

/**
 * Inbound event payload sent to {@code POST /api/message-tracking/publish}.
 *
 * Represents a Kafka message that has been published to a cluster and for
 * which the service must begin tracking POS delivery across all listed stores.
 */
public record PublishEvent(

        /**
         * Unique business identifier for the published message.
         * Used as the primary lookup key throughout the tracking lifecycle.
         */
        @NotBlank(message = "messageKey must not be blank")
        @JsonProperty("messageKey")
        String messageKey,

        /**
         * Identifier of the Kafka cluster to which the message was published.
         */
        @NotBlank(message = "clusterId must not be blank")
        @JsonProperty("clusterId")
        String clusterId,

        /**
         * Kafka partition offset of the published message.
         * Used for OFFSET_MISMATCH detection on re-publish.
         */
        @NotNull(message = "msgOffset must not be null")
        @JsonProperty("msgOffset")
        Long msgOffset,

        /**
         * Absolute expiry timestamp after which store tracking is considered timed-out.
         * Must be in the future relative to the time the event is received.
         */
        @NotNull(message = "expireAt must not be null")
        @JsonProperty("expireAt")
        Instant expireAt,

        /**
         * List of stores that are expected to acknowledge this message.
         * At least one store must be present.
         */
        @NotEmpty(message = "stores must not be empty")
        @Valid
        @JsonProperty("stores")
        List<StoreDetail> stores

) {
}
