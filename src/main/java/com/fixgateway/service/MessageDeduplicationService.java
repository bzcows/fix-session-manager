package com.fixgateway.service;

import com.fixgateway.model.MessageEnvelope;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service for message deduplication using Hazelcast distributed cache.
 * Provides temporal deduplication to prevent processing the same message multiple times.
 *
 * Phase 2: Enhanced with statistics, batch operations, and persistence awareness
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageDeduplicationService {

    private final HazelcastInstance hazelcastInstance;
    
    private IMap<String, DeduplicationRecord> deduplicationMap;
    
    // Local statistics for monitoring (not distributed)
    private final AtomicLong duplicateCount = new AtomicLong(0);
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong cacheHitCount = new AtomicLong(0);
    private final Map<String, Long> duplicateBySession = new ConcurrentHashMap<>();
    
    private static final int DEFAULT_TTL_HOURS = 24;
    private static final String DEDUPLICATION_MAP_NAME = "message-deduplication";
    
    @PostConstruct
    public void initialize() {
        deduplicationMap = hazelcastInstance.getMap(DEDUPLICATION_MAP_NAME);
        log.info("MessageDeduplicationService initialized with map: {} (size: {}, localEndpoint: {})",
                DEDUPLICATION_MAP_NAME, deduplicationMap.size(),
                hazelcastInstance.getLocalEndpoint().getUuid());
    }
    
    /**
     * Check if a message is a duplicate based on its fingerprint.
     * If not a duplicate, record it for future deduplication.
     *
     * @param messageEnvelope The message to check
     * @return true if duplicate, false if new message
     */
    public boolean isDuplicate(MessageEnvelope messageEnvelope) {
        long startTime = System.nanoTime();
        
        try {
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
        } finally {
            long duration = System.nanoTime() - startTime;
            // Record metrics will be handled by the caller (processWithDeduplication)
        }
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
     * Process a message with deduplication and return whether to proceed.
     * This is the main entry point for message processing pipelines.
     *
     * @param messageEnvelope The message to process
     * @return true if message should be processed (not a duplicate), false if duplicate
     */
    public boolean processWithDeduplication(MessageEnvelope messageEnvelope) {
        processedCount.incrementAndGet();
        
        boolean isDuplicate = isDuplicate(messageEnvelope);
        
        if (isDuplicate) {
            duplicateCount.incrementAndGet();
            String sessionId = messageEnvelope.getSessionId();
            if (sessionId != null) {
                duplicateBySession.merge(sessionId, 1L, Long::sum);
            }
        }
        
        return !isDuplicate; // Return true if NOT duplicate (should process)
    }
    
    /**
     * Batch check for duplicates - more efficient for processing multiple messages.
     *
     * @param envelopes List of messages to check
     * @return Map of fingerprint to duplicate status (true = duplicate)
     */
    public Map<String, Boolean> batchCheckDuplicates(java.util.List<MessageEnvelope> envelopes) {
        Map<String, Boolean> results = new java.util.HashMap<>();
        Map<String, String> fingerprints = new java.util.HashMap<>();
        
        // First pass: collect all fingerprints
        for (MessageEnvelope envelope : envelopes) {
            if (envelope != null) {
                String fingerprint = envelope.generateFingerprint();
                fingerprints.put(fingerprint, envelope.getMessageId());
            }
        }
        
        // Batch check with Hazelcast (more efficient than individual calls)
        for (String fingerprint : fingerprints.keySet()) {
            boolean isDuplicate = deduplicationMap.containsKey(fingerprint);
            results.put(fingerprint, isDuplicate);
            if (isDuplicate) {
                cacheHitCount.incrementAndGet();
            }
        }
        
        return results;
    }
    
    /**
     * Get deduplication statistics for monitoring.
     */
    public DeduplicationStats getStatistics() {
        return new DeduplicationStats(
            processedCount.get(),
            duplicateCount.get(),
            cacheHitCount.get(),
            deduplicationMap.size(),
            duplicateBySession,
            Instant.now()
        );
    }
    
    /**
     * Reset local statistics (for testing or monitoring reset).
     */
    public void resetStatistics() {
        duplicateCount.set(0);
        processedCount.set(0);
        cacheHitCount.set(0);
        duplicateBySession.clear();
        log.info("Deduplication statistics reset");
    }
    
    /**
     * Check cache health and size for monitoring.
     */
    public CacheHealth checkCacheHealth() {
        int size = deduplicationMap.size();
        int localPartitionSize = deduplicationMap.localKeySet().size();
        double loadFactor = localPartitionSize > 0 ? (double) size / localPartitionSize : 0;
        
        return new CacheHealth(
            size,
            localPartitionSize,
            loadFactor,
            hazelcastInstance.getCluster().getMembers().size(),
            Instant.now()
        );
    }
    
    /**
     * Statistics DTO for monitoring.
     */
    public static class DeduplicationStats {
        private final long totalProcessed;
        private final long duplicatesDetected;
        private final long cacheHits;
        private final int cacheSize;
        private final Map<String, Long> duplicatesBySession;
        private final Instant timestamp;
        
        public DeduplicationStats(long totalProcessed, long duplicatesDetected, long cacheHits,
                                 int cacheSize, Map<String, Long> duplicatesBySession, Instant timestamp) {
            this.totalProcessed = totalProcessed;
            this.duplicatesDetected = duplicatesDetected;
            this.cacheHits = cacheHits;
            this.cacheSize = cacheSize;
            this.duplicatesBySession = new java.util.HashMap<>(duplicatesBySession);
            this.timestamp = timestamp;
        }
        
        // Getters
        public long getTotalProcessed() { return totalProcessed; }
        public long getDuplicatesDetected() { return duplicatesDetected; }
        public long getCacheHits() { return cacheHits; }
        public int getCacheSize() { return cacheSize; }
        public Map<String, Long> getDuplicatesBySession() { return duplicatesBySession; }
        public Instant getTimestamp() { return timestamp; }
        public double getDuplicateRate() {
            return totalProcessed > 0 ? (double) duplicatesDetected / totalProcessed : 0.0;
        }
    }
    
    /**
     * Cache health DTO for monitoring.
     */
    public static class CacheHealth {
        private final int totalSize;
        private final int localPartitionSize;
        private final double loadFactor;
        private final int clusterSize;
        private final Instant timestamp;
        
        public CacheHealth(int totalSize, int localPartitionSize, double loadFactor,
                          int clusterSize, Instant timestamp) {
            this.totalSize = totalSize;
            this.localPartitionSize = localPartitionSize;
            this.loadFactor = loadFactor;
            this.clusterSize = clusterSize;
            this.timestamp = timestamp;
        }
        
        // Getters
        public int getTotalSize() { return totalSize; }
        public int getLocalPartitionSize() { return localPartitionSize; }
        public double getLoadFactor() { return loadFactor; }
        public int getClusterSize() { return clusterSize; }
        public Instant getTimestamp() { return timestamp; }
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