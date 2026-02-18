package com.pos.inconsistency.scheduler;

import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Daily scheduler that keeps partitioned tables healthy by:
 * <ol>
 *   <li>Creating partitions for the next {@value #DAYS_AHEAD} days (idempotent via
 *       {@code IF NOT EXISTS} guard).</li>
 *   <li>Dropping partitions older than {@value #DAYS_TO_RETAIN} days ago, which are
 *       guaranteed to contain no live rows because {@code expire_at} is always in the
 *       near future.</li>
 * </ol>
 *
 * <p>Without this job the partitioned tables created by V1 migration will fail with
 * <em>"no partition of relation found for row"</em> on day 11.  The {@code DROP TABLE}
 * for old partitions is instant (no VACUUM needed) and frees storage immediately.
 *
 * <p>Runs once per day at 00:05 UTC.  The 5-minute offset avoids contention with
 * any midnight-cron jobs in the surrounding infrastructure.
 */
@Singleton
public class PartitionManagementScheduler {

    private static final Logger log = LoggerFactory.getLogger(PartitionManagementScheduler.class);

    /** How many future daily partitions to keep ready. */
    private static final int DAYS_AHEAD = 14;

    /**
     * Drop partitions whose end date is older than this many days ago.
     * A generous retention of 3 days ensures we never drop a partition that still
     * contains in-flight rows even if the cleanup scheduler is temporarily behind.
     */
    private static final int DAYS_TO_RETAIN = 3;

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy_MM_dd");

    @Inject
    private DataSource dataSource;

    // -----------------------------------------------------------------------
    // Scheduled task
    // -----------------------------------------------------------------------

    /**
     * Creates future partitions and drops old ones.
     * Runs daily at 00:05 UTC.
     */
    @Scheduled(cron = "0 5 0 * * *")
    public void managePartitions() {
        log.info("PartitionManagementScheduler starting");
        int created = 0;
        int dropped = 0;

        try (Connection conn = dataSource.getConnection()) {
            LocalDate today = LocalDate.now();

            // Create partitions for today through today + DAYS_AHEAD
            for (int i = 0; i <= DAYS_AHEAD; i++) {
                LocalDate date = today.plusDays(i);
                if (createPartitionIfAbsent(conn, date)) {
                    created++;
                }
            }

            // Drop partitions older than DAYS_TO_RETAIN days ago
            for (int i = DAYS_TO_RETAIN + 1; i <= DAYS_TO_RETAIN + 30; i++) {
                LocalDate date = today.minusDays(i);
                if (dropPartitionIfExists(conn, date)) {
                    dropped++;
                } else {
                    // Partitions are contiguous — stop once we find a date that has no partition
                    break;
                }
            }
        } catch (SQLException e) {
            log.error("PartitionManagementScheduler failed: {}", e.getMessage(), e);
        }

        log.info("PartitionManagementScheduler done: created={} dropped={}", created, dropped);
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /**
     * Creates mts_summary and mts_store partitions for the given date if they
     * do not already exist.
     *
     * @return true if at least one partition was created
     */
    private boolean createPartitionIfAbsent(Connection conn, LocalDate date) throws SQLException {
        String suffix = date.format(DATE_FMT);
        String start  = date.toString();            // yyyy-MM-dd
        String end    = date.plusDays(1).toString();

        boolean summaryCreated = createSinglePartition(conn,
                "mts_summary", "mts_summary_" + suffix, start, end);
        boolean storeCreated = createSinglePartition(conn,
                "mts_store", "mts_store_" + suffix, start, end);

        return summaryCreated || storeCreated;
    }

    private boolean createSinglePartition(Connection conn,
                                           String parentTable,
                                           String partitionName,
                                           String startDate,
                                           String endDate) throws SQLException {
        // Check existence first to keep the log clean
        final String checkSql = """
                SELECT 1 FROM pg_class c
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE c.relname = ?
                """;

        try (PreparedStatement check = conn.prepareStatement(checkSql)) {
            check.setString(1, partitionName);
            try (var rs = check.executeQuery()) {
                if (rs.next()) {
                    return false; // already exists
                }
            }
        }

        // Partition does not exist — create it
        String ddl = String.format(
                "CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')",
                partitionName, parentTable, startDate, endDate);

        try (PreparedStatement ps = conn.prepareStatement(ddl)) {
            ps.execute();
            log.info("Created partition {}", partitionName);
            return true;
        }
    }

    /**
     * Drops mts_summary and mts_store partitions for the given date if they exist.
     *
     * @return true if at least one partition was dropped (used by the caller to
     *         decide whether to keep scanning backwards)
     */
    private boolean dropPartitionIfExists(Connection conn, LocalDate date) throws SQLException {
        String suffix        = date.format(DATE_FMT);
        String summaryPart   = "mts_summary_" + suffix;
        String storePart     = "mts_store_"   + suffix;

        boolean summaryDropped = dropSinglePartition(conn, summaryPart);
        boolean storeDropped   = dropSinglePartition(conn, storePart);

        return summaryDropped || storeDropped;
    }

    private boolean dropSinglePartition(Connection conn, String partitionName) throws SQLException {
        final String checkSql = """
                SELECT 1 FROM pg_class c
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE c.relname = ?
                """;

        try (PreparedStatement check = conn.prepareStatement(checkSql)) {
            check.setString(1, partitionName);
            try (var rs = check.executeQuery()) {
                if (!rs.next()) {
                    return false; // does not exist
                }
            }
        }

        String ddl = "DROP TABLE IF EXISTS " + partitionName;
        try (PreparedStatement ps = conn.prepareStatement(ddl)) {
            ps.execute();
            log.info("Dropped partition {}", partitionName);
            return true;
        }
    }
}
