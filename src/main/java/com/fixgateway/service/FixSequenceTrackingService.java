package com.fixgateway.service;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Service for tracking FIX protocol sequence numbers to detect protocol violations
 * and prevent duplicate messages during session resets.
 * 
 * This service tracks which message fingerprint is associated with each sequence number
 * per session. If the same sequence number appears with a different message fingerprint,
 * it indicates a FIX protocol violation (sequence number reuse).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FixSequenceTrackingService {

    private final HazelcastInstance hazelcastInstance;
    
    // Hazelcast map: key = "sessionId:msgSeqNum", value = SequenceRecord
    private static final String SEQUENCE_MAP_NAME = "fix-sequence-tracking";
    
    /**
     * Record for tracking sequence number associations
     */
    @lombok.Data
    @lombok.Builder
    private static class SequenceRecord {
        private final String messageFingerprint;
        private final String messageId;
        private final Instant timestamp;
        private final String sessionId;
        private final Integer msgSeqNum;
    }
    
    /**
     * Check if a sequence number has been seen before for this session
     * and track it if not.
     * 
     * @param sessionId FIX session identifier
     * @param msgSeqNum FIX message sequence number
     * @param messageFingerprint Fingerprint of the current message
     * @param messageId Unique message identifier
     * @return true if this is a duplicate or protocol violation, false if new
     */
    public boolean trackSequence(String sessionId, Integer msgSeqNum, 
                                 String messageFingerprint, String messageId) {
        if (sessionId == null || msgSeqNum == null || messageFingerprint == null) {
            log.warn("Invalid parameters for sequence tracking: sessionId={}, msgSeqNum={}, fingerprint={}",
                sessionId, msgSeqNum, messageFingerprint);
            return false;
        }
        
        String mapKey = buildMapKey(sessionId, msgSeqNum);
        IMap<String, SequenceRecord> sequenceMap = hazelcastInstance.getMap(SEQUENCE_MAP_NAME);
        
        SequenceRecord newRecord = SequenceRecord.builder()
            .messageFingerprint(messageFingerprint)
            .messageId(messageId)
            .timestamp(Instant.now())
            .sessionId(sessionId)
            .msgSeqNum(msgSeqNum)
            .build();
        
        // Try to put if absent with TTL (24 hours - typical session lifetime)
        SequenceRecord existingRecord = sequenceMap.putIfAbsent(
            mapKey, newRecord, 24, TimeUnit.HOURS);
        
        if (existingRecord == null) {
            // First time seeing this sequence number for this session
            log.debug("Tracked new sequence number: session={}, seqNum={}, fingerprint={}",
                sessionId, msgSeqNum, messageFingerprint);
            return false;
        }
        
        // Sequence number seen before - check if it's the same message
        if (existingRecord.getMessageFingerprint().equals(messageFingerprint)) {
            // Same message fingerprint - harmless retry or duplicate
            log.debug("Duplicate sequence number with same fingerprint: session={}, seqNum={}, fingerprint={}",
                sessionId, msgSeqNum, messageFingerprint);
            return true;
        } else {
            // DIFFERENT message with same sequence number = FIX protocol violation
            log.error("FIX PROTOCOL VIOLATION: Sequence number reused with different message! " +
                     "session={}, seqNum={}, existingFingerprint={}, newFingerprint={}, existingMsgId={}, newMsgId={}",
                sessionId, msgSeqNum, existingRecord.getMessageFingerprint(), messageFingerprint,
                existingRecord.getMessageId(), messageId);
            
            // Update metrics for protocol violations
            // In production, you might want to increment a counter here
            return true;
        }
    }
    
    /**
     * Check if a sequence number is a duplicate without tracking it.
     * Useful for read-only checks.
     */
    public boolean isDuplicateSequence(String sessionId, Integer msgSeqNum, String messageFingerprint) {
        if (sessionId == null || msgSeqNum == null || messageFingerprint == null) {
            return false;
        }
        
        String mapKey = buildMapKey(sessionId, msgSeqNum);
        IMap<String, SequenceRecord> sequenceMap = hazelcastInstance.getMap(SEQUENCE_MAP_NAME);
        SequenceRecord existingRecord = sequenceMap.get(mapKey);
        
        if (existingRecord == null) {
            return false;
        }
        
        return existingRecord.getMessageFingerprint().equals(messageFingerprint);
    }
    
    /**
     * Remove sequence tracking for a specific session (e.g., on session logout/reset)
     */
    public void clearSessionSequences(String sessionId) {
        if (sessionId == null) {
            return;
        }
        
        IMap<String, SequenceRecord> sequenceMap = hazelcastInstance.getMap(SEQUENCE_MAP_NAME);
        String sessionPrefix = sessionId + ":";
        
        // Remove all entries for this session
        sequenceMap.keySet().stream()
            .filter(key -> key.startsWith(sessionPrefix))
            .forEach(sequenceMap::remove);
        
        log.info("Cleared sequence tracking for session: {}", sessionId);
    }
    
    /**
     * Get statistics about sequence tracking
     */
    public SequenceTrackingStats getStats() {
        IMap<String, SequenceRecord> sequenceMap = hazelcastInstance.getMap(SEQUENCE_MAP_NAME);
        return SequenceTrackingStats.builder()
            .totalTrackedSequences(sequenceMap.size())
            .build();
    }
    
    /**
     * Build the Hazelcast map key for sequence tracking
     */
    private String buildMapKey(String sessionId, Integer msgSeqNum) {
        return sessionId + ":" + msgSeqNum;
    }
    
    /**
     * Statistics for sequence tracking
     */
    @lombok.Data
    @lombok.Builder
    public static class SequenceTrackingStats {
        private final int totalTrackedSequences;
    }
}