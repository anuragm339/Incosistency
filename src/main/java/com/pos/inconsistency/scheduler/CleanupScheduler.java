package com.pos.inconsistency.scheduler;

import com.pos.inconsistency.model.MtsStore;
import com.pos.inconsistency.repository.MtsStoreRepository;
import com.pos.inconsistency.service.MessageTrackingService;
import io.micronaut.context.annotation.Value;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

/**
 * Periodic cleanup scheduler that finalises store tracking rows whose expiry has passed
 * but that were never explicitly finalised through a POS response event.
 *
 * <p>This covers edge cases such as:
 * <ul>
 *   <li>Stores where no POS machine ever responded</li>
 *   <li>Stores where partial responses arrived but the window closed before all responded</li>
 *   <li>Crashes or restarts that left in-flight finalizations incomplete</li>
 * </ul>
 *
 * <p>The scheduler runs every minute (after an initial 30-second delay) and processes
 * up to {@code inconsistency.cleanup-batch-size} rows per invocation to cap the per-run
 * database load.  Each row is finalized independently so that a single bad row cannot
 * block the rest of the batch.
 */
@Singleton
public class CleanupScheduler {

    private static final Logger log = LoggerFactory.getLogger(CleanupScheduler.class);

    @Value("${inconsistency.cleanup-batch-size:500}")
    private int batchSize;

    @Inject
    private MtsStoreRepository storeRepository;

    @Inject
    private MessageTrackingService messageTrackingService;

    // -----------------------------------------------------------------------
    // Scheduled task
    // -----------------------------------------------------------------------

    /**
     * Finds expired-but-active store rows and calls
     * {@link MessageTrackingService#finalizeStore} for each one.
     *
     * Exceptions thrown by individual store finalization are caught and logged —
     * they never propagate to stop the batch.
     *
     * The {@code initialDelay} gives the application time to fully start up and
     * complete Flyway migrations before the first cleanup run.
     */
    @Scheduled(fixedDelay = "1m", initialDelay = "30s")
    public void cleanupExpiredRows() {
        Instant now = Instant.now();
        log.info("CleanupScheduler starting — looking for expired active rows before={} limit={}", now, batchSize);

        List<MtsStore> expiredStores;
        try {
            expiredStores = storeRepository.findExpiredActive(now, batchSize);
        } catch (Exception e) {
            log.error("CleanupScheduler failed to query expired rows: {}", e.getMessage(), e);
            return;
        }

        if (expiredStores.isEmpty()) {
            log.info("CleanupScheduler found no expired active rows");
            return;
        }

        log.info("CleanupScheduler found {} expired active rows to process", expiredStores.size());

        int succeeded = 0;
        int failed = 0;

        for (MtsStore store : expiredStores) {
            try {
                log.info("CleanupScheduler finalizing messageKey={} store={} expireAt={}",
                        store.getMessageKey(), store.getStoreNumber(), store.getExpireAt());
                messageTrackingService.finalizeStore(store);
                succeeded++;
            } catch (Exception e) {
                failed++;
                log.error("CleanupScheduler failed to finalize messageKey={} store={}: {}",
                        store.getMessageKey(), store.getStoreNumber(), e.getMessage(), e);
                // Continue with the next row — one bad row must not stop the batch
            }
        }

        log.info("CleanupScheduler completed batch: succeeded={} failed={} total={}",
                succeeded, failed, expiredStores.size());
    }
}
