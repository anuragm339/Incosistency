package com.pos.inconsistency.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pos.inconsistency.model.MtsStore;
import com.pos.inconsistency.model.PosStatus;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.*;

/**
 * JDBC-based repository for the {@code mts_store} table.
 *
 * JSONB columns ({@code expected_pos}, {@code responded_pos}, {@code missing_pos},
 * {@code pos_statuses}, {@code inconsistencies}) are read as Strings from the
 * {@link ResultSet} and deserialized with Jackson.
 *
 * {@link #findByMessageKeyAndStoreForUpdate} issues {@code SELECT ... FOR UPDATE}
 * and therefore MUST be called within an active transaction.
 */
@Singleton
public class MtsStoreRepository {

    private static final Logger log = LoggerFactory.getLogger(MtsStoreRepository.class);

    private static final TypeReference<List<String>> LIST_STRING = new TypeReference<>() {};
    private static final TypeReference<Map<String, PosStatus>> MAP_POS_STATUS = new TypeReference<>() {};

    private final DataSource dataSource;
    private final ObjectMapper objectMapper;

    @Inject
    public MtsStoreRepository(DataSource dataSource, ObjectMapper objectMapper) {
        this.dataSource = dataSource;
        this.objectMapper = objectMapper;
    }

    // -----------------------------------------------------------------------
    // Read operations
    // -----------------------------------------------------------------------

    /**
     * Finds the store row without locking — safe for read-only queries.
     *
     * @param messageKey  business message identifier
     * @param storeNumber store number
     * @return the store if present
     */
    public Optional<MtsStore> findByMessageKeyAndStore(String messageKey, String storeNumber) {
        final String sql = buildSelectSql(false);
        return executeQuery(sql, messageKey, storeNumber);
    }

    /**
     * Finds the store row with {@code SELECT FOR UPDATE} — must be called inside a transaction.
     *
     * This method uses the provided connection so that the lock remains active until the
     * caller's transaction commits or rolls back.
     *
     * @param conn        active JDBC connection with an open transaction
     * @param messageKey  business message identifier
     * @param storeNumber store number
     * @return the store if present, locked for update
     */
    public Optional<MtsStore> findByMessageKeyAndStoreForUpdate(Connection conn,
                                                                  String messageKey,
                                                                  String storeNumber) {
        final String sql = buildSelectSql(true);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, messageKey);
            ps.setString(2, storeNumber);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRow(rs));
                }
            }
        } catch (SQLException | JsonProcessingException e) {
            log.error("Error in findByMessageKeyAndStoreForUpdate messageKey={} store={}", messageKey, storeNumber, e);
            throw new RuntimeException("DB error in findByMessageKeyAndStoreForUpdate", e);
        }
        return Optional.empty();
    }

    /**
     * Retrieves all store rows associated with a message key (for the status API).
     *
     * @param messageKey business message identifier
     * @return list of store rows (may be empty)
     */
    public List<MtsStore> findAllByMessageKey(String messageKey) {
        final String sql = """
                SELECT id, message_key, cluster_id, store_number, location_id, expire_at,
                       expected_pos, responded_pos, missing_pos, pos_statuses,
                       state, inconsistencies, last_checkpoint_pct, created_at, updated_at
                  FROM mts_store
                 WHERE message_key = ?
                 ORDER BY store_number
                """;

        List<MtsStore> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, messageKey);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(mapRow(rs));
                }
            }
        } catch (SQLException | JsonProcessingException e) {
            log.error("Error in findAllByMessageKey messageKey={}", messageKey, e);
            throw new RuntimeException("DB error in findAllByMessageKey", e);
        }
        return result;
    }

    /**
     * Finds store rows that have passed their expiry and are still in an active state.
     * Used by the cleanup scheduler to handle stores that were never explicitly finalized.
     *
     * @param before rows with {@code expire_at} earlier than this instant qualify
     * @param limit  maximum number of rows to return
     * @return list of expired-but-active store rows
     */
    public List<MtsStore> findExpiredActive(Instant before, int limit) {
        final String sql = """
                SELECT id, message_key, cluster_id, store_number, location_id, expire_at,
                       expected_pos, responded_pos, missing_pos, pos_statuses,
                       state, inconsistencies, last_checkpoint_pct, created_at, updated_at
                  FROM mts_store
                 WHERE expire_at < ?
                   AND state IN ('PENDING', 'PARTIAL')
                 ORDER BY expire_at ASC
                 LIMIT ?
                """;

        List<MtsStore> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setTimestamp(1, Timestamp.from(before));
            ps.setInt(2, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(mapRow(rs));
                }
            }
        } catch (SQLException | JsonProcessingException e) {
            log.error("Error in findExpiredActive before={}", before, e);
            throw new RuntimeException("DB error in findExpiredActive", e);
        }
        return result;
    }

    // -----------------------------------------------------------------------
    // Write operations
    // -----------------------------------------------------------------------

    /**
     * Inserts a new store row.
     *
     * @param store the store to persist (id will be set on return)
     * @return the saved store with its database-generated id
     */
    public MtsStore save(MtsStore store) {
        final String sql = """
                INSERT INTO mts_store
                    (message_key, cluster_id, store_number, location_id, expire_at,
                     expected_pos, responded_pos, missing_pos, pos_statuses,
                     state, inconsistencies, last_checkpoint_pct,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?::jsonb, ?, ?::jsonb, ?, NOW(), NOW())
                RETURNING id, created_at, updated_at
                """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, store.getMessageKey());
            ps.setString(2, store.getClusterId());
            ps.setString(3, store.getStoreNumber());
            setNullableString(ps, 4, store.getLocationId());
            ps.setTimestamp(5, toTimestamp(store.getExpireAt()));
            ps.setString(6, serializeList(store.getExpectedPos()));
            ps.setString(7, serializeList(store.getRespondedPos()));
            ps.setString(8, serializeList(store.getMissingPos()));
            ps.setString(9, serializePosStatuses(store.getPosStatuses()));
            ps.setString(10, store.getState());
            ps.setString(11, serializeList(store.getInconsistencies()));
            ps.setInt(12, store.getLastCheckpointPct());

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    store.setId(rs.getLong("id"));
                    store.setCreatedAt(toInstant(rs.getTimestamp("created_at")));
                    store.setUpdatedAt(toInstant(rs.getTimestamp("updated_at")));
                }
            }

            log.info("Saved mts_store id={} messageKey={} store={}", store.getId(), store.getMessageKey(), store.getStoreNumber());
            return store;

        } catch (SQLException | JsonProcessingException e) {
            log.error("Error saving mts_store messageKey={} store={}", store.getMessageKey(), store.getStoreNumber(), e);
            throw new RuntimeException("DB error in save", e);
        }
    }

    /**
     * Persists an updated store row using the provided connection (supports transactional use).
     *
     * @param conn  active JDBC connection (may be within a transaction)
     * @param store the store to update
     */
    public void update(Connection conn, MtsStore store) {
        final String sql = """
                UPDATE mts_store
                   SET cluster_id          = ?,
                       location_id         = ?,
                       expire_at           = ?,
                       expected_pos        = ?::jsonb,
                       responded_pos       = ?::jsonb,
                       missing_pos         = ?::jsonb,
                       pos_statuses        = ?::jsonb,
                       state               = ?,
                       inconsistencies     = ?::jsonb,
                       last_checkpoint_pct = ?,
                       updated_at          = NOW()
                 WHERE message_key = ?
                   AND store_number = ?
                """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, store.getClusterId());
            setNullableString(ps, 2, store.getLocationId());
            ps.setTimestamp(3, toTimestamp(store.getExpireAt()));
            ps.setString(4, serializeList(store.getExpectedPos()));
            ps.setString(5, serializeList(store.getRespondedPos()));
            ps.setString(6, serializeList(store.getMissingPos()));
            ps.setString(7, serializePosStatuses(store.getPosStatuses()));
            ps.setString(8, store.getState());
            ps.setString(9, serializeList(store.getInconsistencies()));
            ps.setInt(10, store.getLastCheckpointPct());
            ps.setString(11, store.getMessageKey());
            ps.setString(12, store.getStoreNumber());

            int rows = ps.executeUpdate();
            log.info("Updated mts_store messageKey={} store={} rows={}", store.getMessageKey(), store.getStoreNumber(), rows);

        } catch (SQLException | JsonProcessingException e) {
            log.error("Error updating mts_store messageKey={} store={}", store.getMessageKey(), store.getStoreNumber(), e);
            throw new RuntimeException("DB error in update", e);
        }
    }

    /**
     * Convenience overload that acquires its own connection.
     *
     * @param store the store to update
     */
    public void update(MtsStore store) {
        try (Connection conn = dataSource.getConnection()) {
            update(conn, store);
        } catch (SQLException e) {
            log.error("Error acquiring connection for update messageKey={} store={}", store.getMessageKey(), store.getStoreNumber(), e);
            throw new RuntimeException("DB error in update (connection acquire)", e);
        }
    }

    /**
     * Deletes the store row identified by (messageKey, storeNumber).
     *
     * @param messageKey  business message identifier
     * @param storeNumber store number
     */
    public void delete(String messageKey, String storeNumber) {
        final String sql = "DELETE FROM mts_store WHERE message_key = ? AND store_number = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, messageKey);
            ps.setString(2, storeNumber);
            int rows = ps.executeUpdate();
            log.info("Deleted mts_store messageKey={} store={} rows={}", messageKey, storeNumber, rows);

        } catch (SQLException e) {
            log.error("Error deleting mts_store messageKey={} store={}", messageKey, storeNumber, e);
            throw new RuntimeException("DB error in delete", e);
        }
    }

    /**
     * Deletes all store rows belonging to a message key.
     * Used during re-publish (replace) scenarios.
     *
     * @param messageKey business message identifier
     */
    public void deleteAllByMessageKey(String messageKey) {
        final String sql = "DELETE FROM mts_store WHERE message_key = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, messageKey);
            int rows = ps.executeUpdate();
            log.info("Deleted all mts_store rows messageKey={} rows={}", messageKey, rows);

        } catch (SQLException e) {
            log.error("Error deleting all mts_store rows for messageKey={}", messageKey, e);
            throw new RuntimeException("DB error in deleteAllByMessageKey", e);
        }
    }

    /**
     * Batch-inserts a list of store rows in a single {@code executeBatch()} call.
     * More efficient than calling {@link #save(MtsStore)} in a loop.
     *
     * @param stores list of store rows to insert
     */
    public void saveAll(List<MtsStore> stores) {
        if (stores == null || stores.isEmpty()) {
            return;
        }

        final String sql = """
                INSERT INTO mts_store
                    (message_key, cluster_id, store_number, location_id, expire_at,
                     expected_pos, responded_pos, missing_pos, pos_statuses,
                     state, inconsistencies, last_checkpoint_pct,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?::jsonb, ?, ?::jsonb, ?, NOW(), NOW())
                """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            for (MtsStore store : stores) {
                ps.setString(1, store.getMessageKey());
                ps.setString(2, store.getClusterId());
                ps.setString(3, store.getStoreNumber());
                setNullableString(ps, 4, store.getLocationId());
                ps.setTimestamp(5, toTimestamp(store.getExpireAt()));
                ps.setString(6, serializeList(store.getExpectedPos()));
                ps.setString(7, serializeList(store.getRespondedPos()));
                ps.setString(8, serializeList(store.getMissingPos()));
                ps.setString(9, serializePosStatuses(store.getPosStatuses()));
                ps.setString(10, store.getState());
                ps.setString(11, serializeList(store.getInconsistencies()));
                ps.setInt(12, store.getLastCheckpointPct());
                ps.addBatch();
            }

            ps.executeBatch();
            log.info("Batch-inserted {} mts_store rows for messageKey={}", stores.size(),
                    stores.getFirst().getMessageKey());

        } catch (SQLException | JsonProcessingException e) {
            log.error("Error in saveAll for {} stores", stores.size(), e);
            throw new RuntimeException("DB error in saveAll", e);
        }
    }

    // -----------------------------------------------------------------------
    // DataSource accessor — used by services that need transactional control
    // -----------------------------------------------------------------------

    /**
     * Returns the underlying {@link DataSource} so that callers can open a
     * transactional connection and pass it to {@link #findByMessageKeyAndStoreForUpdate}
     * and {@link #update(Connection, MtsStore)}.
     *
     * @return the injected DataSource
     */
    public DataSource getDataSource() {
        return dataSource;
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    private Optional<MtsStore> executeQuery(String sql, String messageKey, String storeNumber) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, messageKey);
            ps.setString(2, storeNumber);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRow(rs));
                }
            }
        } catch (SQLException | JsonProcessingException e) {
            log.error("Error querying mts_store messageKey={} store={}", messageKey, storeNumber, e);
            throw new RuntimeException("DB error in executeQuery", e);
        }
        return Optional.empty();
    }

    private String buildSelectSql(boolean forUpdate) {
        String base = """
                SELECT id, message_key, cluster_id, store_number, location_id, expire_at,
                       expected_pos, responded_pos, missing_pos, pos_statuses,
                       state, inconsistencies, last_checkpoint_pct, created_at, updated_at
                  FROM mts_store
                 WHERE message_key = ?
                   AND store_number = ?
                """;
        return forUpdate ? base + " FOR UPDATE" : base;
    }

    private MtsStore mapRow(ResultSet rs) throws SQLException, JsonProcessingException {
        MtsStore s = new MtsStore();
        s.setId(rs.getLong("id"));
        s.setMessageKey(rs.getString("message_key"));
        s.setClusterId(rs.getString("cluster_id"));
        s.setStoreNumber(rs.getString("store_number"));
        s.setLocationId(rs.getString("location_id"));
        s.setExpireAt(toInstant(rs.getTimestamp("expire_at")));
        s.setExpectedPos(deserializeList(rs.getString("expected_pos")));
        s.setRespondedPos(deserializeList(rs.getString("responded_pos")));
        s.setMissingPos(deserializeList(rs.getString("missing_pos")));
        s.setPosStatuses(deserializePosStatuses(rs.getString("pos_statuses")));
        s.setState(rs.getString("state"));
        s.setInconsistencies(deserializeList(rs.getString("inconsistencies")));
        s.setLastCheckpointPct(rs.getInt("last_checkpoint_pct"));
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

    private Map<String, PosStatus> deserializePosStatuses(String json) throws JsonProcessingException {
        if (json == null || json.isBlank() || "{}".equals(json.trim())) {
            return new HashMap<>();
        }
        return objectMapper.readValue(json, MAP_POS_STATUS);
    }

    private String serializeList(List<String> list) throws JsonProcessingException {
        return objectMapper.writeValueAsString(list == null ? List.of() : list);
    }

    private String serializePosStatuses(Map<String, PosStatus> map) throws JsonProcessingException {
        return objectMapper.writeValueAsString(map == null ? Map.of() : map);
    }

    private static Timestamp toTimestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static Instant toInstant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }

    private static void setNullableString(PreparedStatement ps, int idx, String value) throws SQLException {
        if (value == null) {
            ps.setNull(idx, Types.VARCHAR);
        } else {
            ps.setString(idx, value);
        }
    }
}
