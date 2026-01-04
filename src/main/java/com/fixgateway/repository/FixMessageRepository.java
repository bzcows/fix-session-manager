package com.fixgateway.repository;

import com.fixgateway.model.FixMessageLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface FixMessageRepository extends JpaRepository<FixMessageLog, Long> {
    
    /**
     * Find messages by session ID
     */
    List<FixMessageLog> findBySessionId(String sessionId);
    
    /**
     * Find messages by session ID within a time range
     */
    List<FixMessageLog> findBySessionIdAndCreatedTimestampBetween(
            String sessionId, Instant startTime, Instant endTime);
    
    /**
     * Find messages by message type
     */
    List<FixMessageLog> findByMsgType(String msgType);
    
    /**
     * Find messages by direction (INBOUND/OUTBOUND)
     */
    List<FixMessageLog> findByDirection(FixMessageLog.MessageDirection direction);
    
    /**
     * Find messages by category (APPLICATION/ADMIN)
     */
    List<FixMessageLog> findByCategory(FixMessageLog.MessageCategory category);
    
    /**
     * Count messages by session ID
     */
    Long countBySessionId(String sessionId);
    
    /**
     * Find messages within a time range
     */
    List<FixMessageLog> findByCreatedTimestampBetween(Instant startTime, Instant endTime);
    
    /**
     * Custom batch insert query for better performance
     * Note: This is a native query that matches the table structure
     */
    @Modifying
    @Query(value = "INSERT INTO fix_messages " +
           "(session_id, sender_comp_id, target_comp_id, msg_type, direction, " +
           "message_category, raw_message, parsed_fields, created_timestamp, " +
           "logged_timestamp, node_id, sequence_number, processing_duration_ms) " +
           "VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?)",
           nativeQuery = true)
    void insertBatch(List<Object[]> batch);
    
    /**
     * Delete messages older than specified timestamp
     */
    @Modifying
    @Query("DELETE FROM FixMessageLog f WHERE f.createdTimestamp < :timestamp")
    int deleteOlderThan(@Param("timestamp") Instant timestamp);
    
    /**
     * Get distinct session IDs in the database
     */
    @Query("SELECT DISTINCT f.sessionId FROM FixMessageLog f")
    List<String> findDistinctSessionIds();
}