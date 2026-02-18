package com.pos.inconsistency.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * Represents the delivery status of an individual POS machine within a store.
 *
 * Instances of this class are stored as values in the {@code pos_statuses} JSONB
 * map inside {@link MtsStore}, keyed by the POS hostname.
 *
 * Known status values:
 * <ul>
 *   <li>DONE              – POS machine acknowledged and processed the message</li>
 *   <li>DELIVERED         – Kafka delivery confirmed, consumer ACK received</li>
 *   <li>DELIVERED_NO_ACK  – Kafka delivery confirmed, but no consumer ACK received</li>
 *   <li>FAILED            – Delivery or processing failed</li>
 *   <li>MISSING           – POS machine never responded within the tracking window</li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PosStatus {

    /** Current delivery/processing status for this POS machine. */
    @JsonProperty("status")
    private String status;

    /**
     * Timestamp at which the consumer sent its acknowledgement.
     * Null when no ACK has been received (e.g. DELIVERED_NO_ACK or MISSING).
     */
    @JsonProperty("consumerAckTimestamp")
    private Instant consumerAckTimestamp;

    /** Timestamp at which the PATCH event from this POS was received by this service. */
    @JsonProperty("patchReceivedAt")
    private Instant patchReceivedAt;

    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------

    public PosStatus() {
    }

    public PosStatus(String status, Instant consumerAckTimestamp, Instant patchReceivedAt) {
        this.status = status;
        this.consumerAckTimestamp = consumerAckTimestamp;
        this.patchReceivedAt = patchReceivedAt;
    }

    // -----------------------------------------------------------------------
    // Getters and setters
    // -----------------------------------------------------------------------

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Instant getConsumerAckTimestamp() {
        return consumerAckTimestamp;
    }

    public void setConsumerAckTimestamp(Instant consumerAckTimestamp) {
        this.consumerAckTimestamp = consumerAckTimestamp;
    }

    public Instant getPatchReceivedAt() {
        return patchReceivedAt;
    }

    public void setPatchReceivedAt(Instant patchReceivedAt) {
        this.patchReceivedAt = patchReceivedAt;
    }

    @Override
    public String toString() {
        return "PosStatus{" +
                "status='" + status + '\'' +
                ", consumerAckTimestamp=" + consumerAckTimestamp +
                ", patchReceivedAt=" + patchReceivedAt +
                '}';
    }
}
