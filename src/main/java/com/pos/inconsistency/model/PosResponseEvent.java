package com.pos.inconsistency.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * Inbound event payload sent to {@code PATCH /api/message-tracking/{messageKey}/pos-response}.
 *
 * Represents the response from a single POS machine confirming (or failing)
 * delivery of the message identified by the path variable {@code messageKey}.
 */
public record PosResponseEvent(

        /**
         * Hostname of the POS machine that generated this response.
         * Must match one of the hostnames in {@link MtsStore#getExpectedPos()}.
         */
        @NotBlank(message = "posHostname must not be blank")
        @JsonProperty("posHostname")
        String posHostname,

        /**
         * Store number the POS machine belongs to.
         * Used together with the path-variable messageKey to locate the correct
         * {@link MtsStore} row.
         */
        @NotBlank(message = "storeNumber must not be blank")
        @JsonProperty("storeNumber")
        String storeNumber,

        /**
         * Whether the Kafka consumer successfully delivered the message to this POS.
         */
        @NotNull(message = "delivered must not be null")
        @JsonProperty("delivered")
        boolean delivered,

        /**
         * ISO-8601 string representing when the consumer sent its acknowledgement.
         * May be null if the POS machine did not generate an ACK (e.g. DELIVERED_NO_ACK).
         */
        @JsonProperty("consumerAckTimestamp")
        String consumerAckTimestamp,

        /**
         * Delivery/processing status string reported by the POS machine.
         * Expected values: DONE, DELIVERED, DELIVERED_NO_ACK, FAILED.
         */
        @NotBlank(message = "status must not be blank")
        @JsonProperty("status")
        String status

) {
}
