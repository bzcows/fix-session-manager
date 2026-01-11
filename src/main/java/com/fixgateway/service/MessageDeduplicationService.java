package com.fixgateway.service;

import com.fixgateway.model.MessageEnvelope;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * Service for message deduplication using Hazelcast distributed cache.
 * Provides temporal deduplication to prevent processing the same message multiple times.
 * 
 * Phase 1: Basic fingerprint-based deduplication with TTL
 * Phase 2: Enhanced with database persistence and recovery
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageDeduplicationService {

    private final HazelcastInstance hazelcastInstance;
    
    private IMap<String, DeduplicationRecord> deduplicationMap;
    
    private static final int DEFAULT_TTL_HOURS = 24;
    private static final String DEDUPLICATION_MAP_NAME = "message-deduplication";
    
    @PostConstruct
    public void initialize() {
        deduplicationMap = hazelcastInstance.getMap(DEDUPLICATION_MAP_NAME);
        log.info("MessageDeduplicationService initialized with map: {}", DEDUPLICATION_MAP_NAME);
    }
    
    /**
     * Check if a message is a duplicate based on its fingerprint.
     * If not a duplicate, record it for future deduplication.
     * 
     * @param messageEnvelope The message to check
     * @return true if duplicate, false if new message
     */
    public boolean isDuplicate(MessageEnvelope messageEnvelope) {
        if (messageEnvelope == null) {
            return false;
        }
        
        String fingerprint = messageEnvelope.generateFingerprint();
        String messageId = messageEnvelope.getMessageId();
        
        // Try to put the record atomically
        DeduplicationRecord newRecord = new DeduplicationRecord(
            messageId,
            messageEnvelope.getSessionId(),
            messageEnvelope.getMsgType(),
            System.currentTimeMillis()
        );
        
        DeduplicationRecord existingRecord = deduplicationMap.putIfAbsent(
            fingerprint, 
            newRecord,
            DEFAULT_TTL_HOURS, 
            TimeUnit.HOURS
        );
        
        if (existingRecord != null) {
            log.warn("Duplicate message detected: fingerprint={}, messageId={}, existingMessageId={}, session={}, msgType={}",
                fingerprint, messageId, existingRecord.getMessageId(),
                messageEnvelope.getSessionId(), messageEnvelope.getMsgType());
            return true;
        }
        
        log.debug("Recorded new message for deduplication: fingerprint={}, messageId={}, session={}",
            fingerprint, messageId, messageEnvelope.getSessionId());
        return false;
    }
    
    /**
     * Check if a message is a duplicate without recording it.
     * Useful for read-only checks.
     */
    public boolean checkDuplicate(MessageEnvelope messageEnvelope) {
        if (messageEnvelope == null) {
            return false;
        }
        
        String fingerprint = messageEnvelope.generateFingerprint();
        return deduplicationMap.containsKey(fingerprint);
    }
    
    /**
     * Remove a message from deduplication cache.
     * Useful for manual cleanup or testing.
     */
    public void removeFromCache(MessageEnvelope messageEnvelope) {
        if (messageEnvelope == null) {
            return;
        }
        
        String fingerprint = messageEnvelope.generateFingerprint();
        deduplicationMap.remove(fingerprint);
        log.debug("Removed message from deduplication cache: fingerprint={}", fingerprint);
    }
    
    /**
     * Get current cache size for monitoring.
     */
    public int getCacheSize() {
        return deduplicationMap.size();
    }
    
    /**
     * Clear the entire deduplication cache.
     * Use with caution - only for testing or emergency recovery.
     */
    public void clearCache() {
        deduplicationMap.clear();
        log.warn("Cleared entire deduplication cache");
    }
    
    /**
     * Record for tracking deduplication metadata.
     */
    private static class DeduplicationRecord {
        private final String messageId;
        private final String sessionId;
        private final String msgType;
        private final long timestamp;
        
        public DeduplicationRecord(String messageId, String sessionId, String msgType, long timestamp) {
            this.messageId = messageId;
            this.sessionId = sessionId;
            this.msgType = msgType;
            this.timestamp = timestamp;
        }
        
        public String getMessageId() {
            return messageId;
        }
        
        public String getSessionId() {
            return sessionId;
        }
        
        public String getMsgType() {
            return msgType;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
        
        @Override
        public String toString() {
            return String.format("DeduplicationRecord{messageId='%s', sessionId='%s', msgType='%s', timestamp=%d}",
                messageId, sessionId, msgType, timestamp);
        }
    }
}