package com.pos.inconsistency.model;

/**
 * Enumeration of all inconsistency types that can be detected by the service.
 *
 * <ul>
 *   <li>DUPLICATE_KEY         - The same messageKey was published more than once.</li>
 *   <li>LATE_POS_RESPONSE     - A POS machine responded after the late-threshold window.</li>
 *   <li>CONFLICTING_STATUS    - A store's posStatuses map contains both DONE and DELIVERED_NO_ACK.</li>
 *   <li>MISSING_POS           - One or more expected POS machines never responded.</li>
 *   <li>CONSUMER_ACK_MISSING  - A POS machine reported DONE or DELIVERED but has no consumer ACK timestamp.</li>
 *   <li>MISSING_STORE         - One or more stores timed out without any POS response at all.</li>
 *   <li>OFFSET_MISMATCH       - A re-published message arrived with a different Kafka offset.</li>
 *   <li>PREMATURE_REPUBLISH   - A message was re-published before the previous tracking window closed.</li>
 * </ul>
 */
public enum InconsistencyType {

    DUPLICATE_KEY,
    LATE_POS_RESPONSE,
    CONFLICTING_STATUS,
    MISSING_POS,
    CONSUMER_ACK_MISSING,
    MISSING_STORE,
    OFFSET_MISMATCH,
    PREMATURE_REPUBLISH
}
