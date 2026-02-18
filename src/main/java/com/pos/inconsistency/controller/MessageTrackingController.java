package com.pos.inconsistency.controller;

import com.pos.inconsistency.model.PublishEvent;
import com.pos.inconsistency.model.PosResponseEvent;
import com.pos.inconsistency.model.StatusResponse;
import com.pos.inconsistency.service.MessageTrackingService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.micronaut.http.exceptions.HttpStatusException;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST controller that exposes the message-tracking API.
 *
 * Base path: {@code /api/message-tracking}
 *
 * Endpoints:
 * <ul>
 *   <li>{@code POST  /api/message-tracking/publish}                     – register a new publish event</li>
 *   <li>{@code PATCH /api/message-tracking/{messageKey}/pos-response}   – record a POS machine response</li>
 *   <li>{@code GET   /api/message-tracking/{messageKey}/status}         – query current tracking state</li>
 * </ul>
 */
@Controller("/api/message-tracking")
public class MessageTrackingController {

    private static final Logger log = LoggerFactory.getLogger(MessageTrackingController.class);

    @Inject
    private MessageTrackingService messageTrackingService;

    // -----------------------------------------------------------------------
    // POST /api/message-tracking/publish
    // -----------------------------------------------------------------------

    /**
     * Registers a new publish event and starts tracking.
     *
     * Returns HTTP 202 Accepted to indicate that the event has been received
     * and will be processed (tracking initialised synchronously, but NR emission
     * may be deferred).
     *
     * @param event the validated publish event body
     */
    @Post("/publish")
    @Status(HttpStatus.ACCEPTED)
    public void handlePublish(@Body @Valid PublishEvent event) {
        log.info("POST /publish messageKey={} clusterId={}", event.messageKey(), event.clusterId());
        messageTrackingService.handlePublish(event);
    }

    // -----------------------------------------------------------------------
    // PATCH /api/message-tracking/{messageKey}/pos-response
    // -----------------------------------------------------------------------

    /**
     * Records a POS machine's response for the given message key.
     *
     * Returns HTTP 200 OK once the response has been processed and the store state updated.
     *
     * @param messageKey the message identifier from the URL path
     * @param event      the validated POS response body
     */
    @Patch("/{messageKey}/pos-response")
    @Status(HttpStatus.OK)
    public void handlePosResponse(@PathVariable String messageKey,
                                   @Body @Valid PosResponseEvent event) {
        log.info("PATCH /{}/pos-response store={} pos={}", messageKey, event.storeNumber(), event.posHostname());
        messageTrackingService.handlePosResponse(messageKey, event);
    }

    // -----------------------------------------------------------------------
    // GET /api/message-tracking/{messageKey}/status
    // -----------------------------------------------------------------------

    /**
     * Returns the current tracking status for a message key.
     *
     * Returns HTTP 200 with the status body, or HTTP 404 if the messageKey
     * does not have an active tracking row.
     *
     * @param messageKey the message identifier from the URL path
     * @return HTTP 200 with {@link StatusResponse}, or HTTP 404
     */
    @Get("/{messageKey}/status")
    public HttpResponse<StatusResponse> getStatus(@PathVariable String messageKey) {
        log.info("GET /{}/status", messageKey);
        try {
            StatusResponse status = messageTrackingService.getStatus(messageKey);
            return HttpResponse.ok(status);
        } catch (HttpStatusException e) {
            if (e.getStatus() == HttpStatus.NOT_FOUND) {
                log.info("Status not found for messageKey={}", messageKey);
                return HttpResponse.notFound();
            }
            log.error("Unexpected HTTP error for getStatus messageKey={}", messageKey, e);
            throw e;
        }
    }
}
