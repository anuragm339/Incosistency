package com.pos.inconsistency.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pos.inconsistency.model.MtsSummary;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * JDBC-based repository for the {@code mts_summary} table.
 *
 * All SQL is hand-written using {@link PreparedStatement} for full control
 * over partition-pruning and JSONB handling.  JSONB columns are read from
 * the ResultSet as plain Strings and deserialized with Jackson.
 */
@Singleton
public class MtsSummaryRepository {

    private static final Logger log = LoggerFactory.getLogger(MtsSummaryRepository.class);

    private static final TypeReference<List<String>> LIST_STRING = new TypeReference<>() {};

    private final DataSource dataSource;
    private final ObjectMapper objectMapper;

    @Inject
    public MtsSummaryRepository(DataSource dataSource, ObjectMapper objectMapper) {
        this.dataSource = dataSource;
        this.objectMapper = objectMapper;
    }

    // -----------------------------------------------------------------------
    // Read operations
    // -----------------------------------------------------------------------

    /**
     * Finds a summary row by its business key.
     *
     * @param messageKey unique business identifier
     * @return the summary if present
     */
    public Optional<MtsSummary> findByMessageKey(String messageKey) {
        final String sql = """
                SELECT id, message_key, cluster_id, msg_offset,
                       first_published_at, last_published_at, expire_at,
                       total_stores, stores_done, stores_partial, stores_timed_out,
                       publish_count, state, inconsistencies,
                       created_at, updated_at
                  FROM mts_summary
                 WHERE message_key = ?
                """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, messageKey);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRow(rs));
                }
            }
        } catch (SQLException | JsonProcessingException e) {
            log.error("Error fetching mts_summary for messageKey={}", messageKey, e);
            throw new RuntimeException("DB error in findByMessageKey", e);
        }
        return Optional.empty();
    }

    // -----------------------------------------------------------------------
    // Write operations
    // -----------------------------------------------------------------------

    /**
     * Inserts a new summary row using the provided connection (supports transactional use).
     *
     * @param conn    active JDBC connection (may be within a transaction)
     * @param summary the summary to persist (id field will be populated on return)
     * @return the saved summary with its database-generated id
     */
    public MtsSummary save(Connection conn, MtsSummary summary) {
        final String sql = """
                INSERT INTO mts_summary
                    (message_key, cluster_id, msg_offset,
                     first_published_at, last_published_at, expire_at,
                     total_stores, stores_done, stores_partial, stores_timed_out,
                     publish_count, state, inconsistencies,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, NOW(), NOW())
                RETURNING id, created_at, updated_at
                """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, summary.getMessageKey());
            ps.setString(2, summary.getClusterId());
            setNullableLong(ps, 3, summary.getMsgOffset());
            ps.setTimestamp(4, toTimestamp(summary.getFirstPublishedAt()));
            ps.setTimestamp(5, toTimestamp(summary.getLastPublishedAt()));
            ps.setTimestamp(6, toTimestamp(summary.getExpireAt()));
            ps.setInt(7, summary.getTotalStores());
            ps.setInt(8, summary.getStoresDone());
            ps.setInt(9, summary.getStoresPartial());
            ps.setInt(10, summary.getStoresTimedOut());
            ps.setInt(11, summary.getPublishCount());
            ps.setString(12, summary.getState());
            ps.setString(13, serializeList(summary.getInconsistencies()));

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    summary.setId(rs.getLong("id"));
                    summary.setCreatedAt(toInstant(rs.getTimestamp("created_at")));
                    summary.setUpdatedAt(toInstant(rs.getTimestamp("updated_at")));
                }
            }
            log.info("Saved mts_summary id={} messageKey={}", summary.getId(), summary.getMessageKey());
            return summary;
        } catch (SQLException | JsonProcessingException e) {
            log.error("Error saving mts_summary(conn) for messageKey={}", summary.getMessageKey(), e);
            throw new RuntimeException("DB error in save(conn)", e);
        }
    }

    /**
     * Deletes the summary row for the given messageKey using the provided connection
     * (supports transactional use).
     *
     * @param conn       active JDBC connection (may be within a transaction)
     * @param messageKey business key to delete
     */
    public void delete(Connection conn, String messageKey) {
        final String sql = "DELETE FROM mts_summary WHERE message_key = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, messageKey);
            int rows = ps.executeUpdate();
            log.info("Deleted mts_summary(conn) messageKey={} rows={}", messageKey, rows);
        } catch (SQLException e) {
            log.error("Error in delete(conn) for messageKey={}", messageKey, e);
            throw new RuntimeException("DB error in delete(conn)", e);
        }
    }

    /**
     * Returns the underlying {@link DataSource} so callers can share a single
     * transactional connection across summary and store operations.
     *
     * @return the injected DataSource
     */
    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * Inserts a new summary row and returns the entity with the generated id.
     *
     * @param summary the summary to persist (id field will be populated on return)
     * @return the saved summary with its database-generated id
     */
    public MtsSummary save(MtsSummary summary) {
        final String sql = """
                INSERT INTO mts_summary
                    (message_key, cluster_id, msg_offset,
                     first_published_at, last_published_at, expire_at,
                     total_stores, stores_done, stores_partial, stores_timed_out,
                     publish_count, state, inconsistencies,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, NOW(), NOW())
                RETURNING id, created_at, updated_at
                """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, summary.getMessageKey());
            ps.setString(2, summary.getClusterId());
            setNullableLong(ps, 3, summary.getMsgOffset());
            ps.setTimestamp(4, toTimestamp(summary.getFirstPublishedAt()));
            ps.setTimestamp(5, toTimestamp(summary.getLastPublishedAt()));
            ps.setTimestamp(6, toTimestamp(summary.getExpireAt()));
            ps.setInt(7, summary.getTotalStores());
            ps.setInt(8, summary.getStoresDone());
            ps.setInt(9, summary.getStoresPartial());
            ps.setInt(10, summary.getStoresTimedOut());
            ps.setInt(11, summary.getPublishCount());
            ps.setString(12, summary.getState());
            ps.setString(13, serializeList(summary.getInconsistencies()));

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    summary.setId(rs.getLong("id"));
                    summary.setCreatedAt(toInstant(rs.getTimestamp("created_at")));
                    summary.setUpdatedAt(toInstant(rs.getTimestamp("updated_at")));
                }
            }

            log.info("Saved mts_summary id={} messageKey={}", summary.getId(), summary.getMessageKey());
            return summary;

        } catch (SQLException | JsonProcessingException e) {
            log.error("Error saving mts_summary for messageKey={}", summary.getMessageKey(), e);
            throw new RuntimeException("DB error in save", e);
        }
    }

    /**
     * Updates all mutable fields of an existing summary row.
     *
     * @param summary the summary to update (must have a valid id)
     */
    public void update(MtsSummary summary) {
        final String sql = """
                UPDATE mts_summary
                   SET cluster_id         = ?,
                       msg_offset         = ?,
                       first_published_at = ?,
                       last_published_at  = ?,
                       expire_at          = ?,
                       total_stores       = ?,
                       stores_done        = ?,
                       stores_partial     = ?,
                       stores_timed_out   = ?,
                       publish_count      = ?,
                       state              = ?,
                       inconsistencies    = ?::jsonb,
                       updated_at         = NOW()
                 WHERE message_key = ?
                """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, summary.getClusterId());
            setNullableLong(ps, 2, summary.getMsgOffset());
            ps.setTimestamp(3, toTimestamp(summary.getFirstPublishedAt()));
            ps.setTimestamp(4, toTimestamp(summary.getLastPublishedAt()));
            ps.setTimestamp(5, toTimestamp(summary.getExpireAt()));
            ps.setInt(6, summary.getTotalStores());
            ps.setInt(7, summary.getStoresDone());
            ps.setInt(8, summary.getStoresPartial());
            ps.setInt(9, summary.getStoresTimedOut());
            ps.setInt(10, summary.getPublishCount());
            ps.setString(11, summary.getState());
            ps.setString(12, serializeList(summary.getInconsistencies()));
            ps.setString(13, summary.getMessageKey());

            int rows = ps.executeUpdate();
            log.info("Updated mts_summary messageKey={} rows={}", summary.getMessageKey(), rows);

        } catch (SQLException | JsonProcessingException e) {
            log.error("Error updating mts_summary for messageKey={}", summary.getMessageKey(), e);
            throw new RuntimeException("DB error in update", e);
        }
    }

    /**
     * Deletes the summary row for the given messageKey.
     *
     * @param messageKey business key to delete
     */
    public void delete(String messageKey) {
        final String sql = "DELETE FROM mts_summary WHERE message_key = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, messageKey);
            int rows = ps.executeUpdate();
            log.info("Deleted mts_summary messageKey={} rows={}", messageKey, rows);

        } catch (SQLException e) {
            log.error("Error deleting mts_summary for messageKey={}", messageKey, e);
            throw new RuntimeException("DB error in delete", e);
        }
    }

    // -----------------------------------------------------------------------
    // Atomic counter increments — avoid full UPDATE round-trips
    // -----------------------------------------------------------------------

    /**
     * Atomically increments {@code stores_done} by 1.
     * Transitions state to DONE when all stores are accounted for.
     *
     * @param messageKey the message to update
     */
    public void incrementStoresDone(String messageKey) {
        final String sql = """
                UPDATE mts_summary
                   SET stores_done = stores_done + 1,
                       state = CASE
                           WHEN (stores_done + 1 + stores_partial + stores_timed_out) >= total_stores
                               THEN 'DONE'
                           ELSE state
                       END,
                       updated_at = NOW()
                 WHERE message_key = ?
                """;
        executeCounterUpdate(sql, messageKey, "incrementStoresDone");
    }

    /**
     * Transactional overload for {@link #incrementStoresDone(String)}.
     * Uses the provided connection to participate in the caller's transaction.
     */
    public void incrementStoresDone(Connection conn, String messageKey) {
        final String sql = """
                UPDATE mts_summary
                   SET stores_done = stores_done + 1,
                       state = CASE
                           WHEN (stores_done + 1 + stores_partial + stores_timed_out) >= total_stores
                               THEN 'DONE'
                           ELSE state
                       END,
                       updated_at = NOW()
                 WHERE message_key = ?
                """;
        executeCounterUpdate(conn, sql, messageKey, "incrementStoresDone(conn)");
    }

    /**
     * Atomically increments {@code stores_timed_out} by 1.
     *
     * @param messageKey the message to update
     */
    public void incrementStoresTimedOut(String messageKey) {
        final String sql = """
                UPDATE mts_summary
                   SET stores_timed_out = stores_timed_out + 1,
                       updated_at = NOW()
                 WHERE message_key = ?
                """;
        executeCounterUpdate(sql, messageKey, "incrementStoresTimedOut");
    }

    /**
     * Transactional overload for {@link #incrementStoresTimedOut(String)}.
     * Uses the provided connection to participate in the caller's transaction.
     */
    public void incrementStoresTimedOut(Connection conn, String messageKey) {
        final String sql = """
                UPDATE mts_summary
                   SET stores_timed_out = stores_timed_out + 1,
                       updated_at = NOW()
                 WHERE message_key = ?
                """;
        executeCounterUpdate(conn, sql, messageKey, "incrementStoresTimedOut(conn)");
    }

    /**
     * Atomically increments {@code stores_partial} by 1.
     *
     * @param messageKey the message to update
     */
    public void incrementStoresPartial(String messageKey) {
        final String sql = """
                UPDATE mts_summary
                   SET stores_partial = stores_partial + 1,
                       updated_at = NOW()
                 WHERE message_key = ?
                """;
        executeCounterUpdate(sql, messageKey, "incrementStoresPartial");
    }

    /**
     * Transactional overload for {@link #incrementStoresPartial(String)}.
     * Uses the provided connection to participate in the caller's transaction.
     */
    public void incrementStoresPartial(Connection conn, String messageKey) {
        final String sql = """
                UPDATE mts_summary
                   SET stores_partial = stores_partial + 1,
                       updated_at = NOW()
                 WHERE message_key = ?
                """;
        executeCounterUpdate(conn, sql, messageKey, "incrementStoresPartial(conn)");
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    private void executeCounterUpdate(String sql, String messageKey, String opName) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, messageKey);
            int rows = ps.executeUpdate();
            log.info("{} messageKey={} rows={}", opName, messageKey, rows);

        } catch (SQLException e) {
            log.error("Error in {} for messageKey={}", opName, messageKey, e);
            throw new RuntimeException("DB error in " + opName, e);
        }
    }

    private void executeCounterUpdate(Connection conn, String sql, String messageKey, String opName) {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, messageKey);
            int rows = ps.executeUpdate();
            log.info("{} messageKey={} rows={}", opName, messageKey, rows);
        } catch (SQLException e) {
            log.error("Error in {} for messageKey={}", opName, messageKey, e);
            throw new RuntimeException("DB error in " + opName, e);
        }
    }

    private MtsSummary mapRow(ResultSet rs) throws SQLException, JsonProcessingException {
        MtsSummary s = new MtsSummary();
        s.setId(rs.getLong("id"));
        s.setMessageKey(rs.getString("message_key"));
        s.setClusterId(rs.getString("cluster_id"));

        long offset = rs.getLong("msg_offset");
        s.setMsgOffset(rs.wasNull() ? null : offset);

        s.setFirstPublishedAt(toInstant(rs.getTimestamp("first_published_at")));
        s.setLastPublishedAt(toInstant(rs.getTimestamp("last_published_at")));
        s.setExpireAt(toInstant(rs.getTimestamp("expire_at")));
        s.setTotalStores(rs.getInt("total_stores"));
        s.setStoresDone(rs.getInt("stores_done"));
        s.setStoresPartial(rs.getInt("stores_partial"));
        s.setStoresTimedOut(rs.getInt("stores_timed_out"));
        s.setPublishCount(rs.getInt("publish_count"));
        s.setState(rs.getString("state"));

        String inconsistenciesJson = rs.getString("inconsistencies");
        s.setInconsistencies(deserializeList(inconsistenciesJson));

        s.setCreatedAt(toInstant(rs.getTimestamp("created_at")));
        s.setUpdatedAt(toInstant(rs.getTimestamp("updated_at")));
        return s;
    }

    private List<String> deserializeList(String json) throws JsonProcessingException {
        if (json == null || json.isBlank()) {
            return new ArrayList<>();
        }
        return objectMapper.readValue(json, LIST_STRING);
    }

    private String serializeList(List<String> list) throws JsonProcessingException {
        if (list == null) {
            return "[]";
        }
        return objectMapper.writeValueAsString(list);
    }

    private static Timestamp toTimestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static Instant toInstant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }

    private static void setNullableLong(PreparedStatement ps, int idx, Long value) throws SQLException {
        if (value == null) {
            ps.setNull(idx, Types.BIGINT);
        } else {
            ps.setLong(idx, value);
        }
    }
}
