package com.pos.inconsistency.scheduler;

import com.pos.inconsistency.model.MtsSummary;
import com.pos.inconsistency.model.MtsStore;
import com.pos.inconsistency.repository.MtsSummaryRepository;
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

    @Value("${inconsistency.finalizing-stale-minutes:5}")
    private int finalizingStaleMinutes;

    @Inject
    private MtsSummaryRepository summaryRepository;

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

        // ---- Pass 0: retry stale FINALIZING summary rows -------------------------
        // If finalizeMessage claimed a summary row but then failed (e.g. transient DB
        // error on delete), the row stays FINALIZING with no other code path to retry
        // it.  This pass re-triggers finalizeMessage for any summary stuck > 5 min.
        List<MtsSummary> staleFinalizingSummaries;
        try {
            staleFinalizingSummaries = summaryRepository.findStaleFinalizing(finalizingStaleMinutes, batchSize);
        } catch (Exception e) {
            log.error("CleanupScheduler failed to query stale FINALIZING summaries: {}", e.getMessage(), e);
            staleFinalizingSummaries = List.of();
        }

        if (!staleFinalizingSummaries.isEmpty()) {
            log.info("CleanupScheduler found {} stale FINALIZING summaries to retry", staleFinalizingSummaries.size());
            int succeeded = 0;
            int failed = 0;
            for (MtsSummary summary : staleFinalizingSummaries) {
                try {
                    log.info("CleanupScheduler retrying finalizeMessage messageKey={}", summary.getMessageKey());
                    messageTrackingService.finalizeMessage(summary);
                    succeeded++;
                } catch (Exception e) {
                    failed++;
                    log.error("CleanupScheduler failed to retry finalizeMessage messageKey={}: {}",
                            summary.getMessageKey(), e.getMessage(), e);
                }
            }
            log.info("CleanupScheduler summary retry batch: succeeded={} failed={} total={}",
                    succeeded, failed, staleFinalizingSummaries.size());
        }

        // ---- Pass 1: stale FINALIZING store rows ---------------------------------
        List<MtsStore> staleFinalizing;
        try {
            staleFinalizing = storeRepository.findStaleFinalizing(finalizingStaleMinutes, batchSize);
        } catch (Exception e) {
            log.error("CleanupScheduler failed to query stale FINALIZING rows: {}", e.getMessage(), e);
            staleFinalizing = List.of();
        }

        if (!staleFinalizing.isEmpty()) {
            log.info("CleanupScheduler found {} stale FINALIZING rows to process", staleFinalizing.size());
            finalizeBatch(staleFinalizing);
        }

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
        finalizeBatch(expiredStores);
    }

    private void finalizeBatch(List<MtsStore> stores) {
        int succeeded = 0;
        int failed = 0;

        for (MtsStore store : stores) {
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
                succeeded, failed, stores.size());
    }
}
