package com.fixgateway.metrics;

import com.fixgateway.service.MessageDeduplicationService;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics collector for message deduplication subsystem.
 * Exposes deduplication effectiveness metrics for monitoring and alerting.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeduplicationMetrics {

    private final MeterRegistry meterRegistry;
    private final com.hazelcast.core.HazelcastInstance hazelcastInstance;
    
    // Atomic counters for real-time metrics
    private final AtomicLong duplicatesDetected = new AtomicLong(0);
    private final AtomicLong messagesProcessed = new AtomicLong(0);
    private final AtomicLong cacheHits = new AtomicLong(0);
    private final AtomicLong cacheMisses = new AtomicLong(0);
    
    // Timers for performance monitoring
    private Timer deduplicationCheckTimer;
    private Timer cacheOperationTimer;
    
    @PostConstruct
    public void init() {
        // Initialize timers
        deduplicationCheckTimer = Timer.builder("fix.deduplication.check.duration")
            .description("Duration of deduplication check operations")
            .register(meterRegistry);
        
        cacheOperationTimer = Timer.builder("fix.deduplication.cache.operation.duration")
            .description("Duration of cache operations (put/get)")
            .register(meterRegistry);
        
        // Register gauges for real-time metrics
        Gauge.builder("fix.deduplication.messages.processed", messagesProcessed, AtomicLong::get)
            .description("Total number of messages processed for deduplication")
            .register(meterRegistry);
        
        Gauge.builder("fix.deduplication.duplicates.detected", duplicatesDetected, AtomicLong::get)
            .description("Total number of duplicate messages detected")
            .register(meterRegistry);
        
        Gauge.builder("fix.deduplication.cache.hits", cacheHits, AtomicLong::get)
            .description("Total number of cache hits (duplicate found in cache)")
            .register(meterRegistry);
        
        Gauge.builder("fix.deduplication.cache.misses", cacheMisses, AtomicLong::get)
            .description("Total number of cache misses (new message)")
            .register(meterRegistry);
        
        // Dynamic gauge for cache size (pulled directly from Hazelcast)
        Gauge.builder("fix.deduplication.cache.size", this,
                metrics -> {
                    try {
                        var map = hazelcastInstance.getMap("message-deduplication");
                        return map.size();
                    } catch (Exception e) {
                        log.debug("Failed to get cache size from Hazelcast", e);
                        return 0;
                    }
                })
            .description("Current size of deduplication cache")
            .register(meterRegistry);
        
        // Dynamic gauge for duplicate rate (calculated from local counters)
        Gauge.builder("fix.deduplication.duplicate.rate", this,
                metrics -> {
                    long processed = messagesProcessed.get();
                    return processed > 0 ? (double) duplicatesDetected.get() / processed : 0.0;
                })
            .description("Duplicate detection rate (0.0-1.0)")
            .register(meterRegistry);
        
        log.info("Deduplication metrics initialized");
    }
    
    /**
     * Record a deduplication check operation.
     */
    public void recordDeduplicationCheck(long duration, TimeUnit unit, boolean isDuplicate) {
        deduplicationCheckTimer.record(duration, unit);
        messagesProcessed.incrementAndGet();
        
        if (isDuplicate) {
            duplicatesDetected.incrementAndGet();
            cacheHits.incrementAndGet();
        } else {
            cacheMisses.incrementAndGet();
        }
    }
    
    /**
     * Record a cache operation (put/get).
     */
    public void recordCacheOperation(long duration, TimeUnit unit) {
        cacheOperationTimer.record(duration, unit);
    }
    
    /**
     * Increment duplicates detected counter.
     */
    public void incrementDuplicatesDetected(long count) {
        duplicatesDetected.addAndGet(count);
    }
    
    /**
     * Increment messages processed counter.
     */
    public void incrementMessagesProcessed(long count) {
        messagesProcessed.addAndGet(count);
    }
    
    /**
     * Get current metrics snapshot.
     */
    public MetricsSnapshot getSnapshot() {
        int cacheSize = 0;
        int cacheTotalSize = 0;
        int cacheLocalPartitionSize = 0;
        double cacheLoadFactor = 0.0;
        int clusterSize = 0;
        
        try {
            var map = hazelcastInstance.getMap("message-deduplication");
            cacheSize = map.size();
            cacheLocalPartitionSize = map.localKeySet().size();
            cacheTotalSize = cacheSize;
            cacheLoadFactor = cacheLocalPartitionSize > 0 ? (double) cacheSize / cacheLocalPartitionSize : 0.0;
            clusterSize = hazelcastInstance.getCluster().getMembers().size();
        } catch (Exception e) {
            log.debug("Failed to get cache metrics from Hazelcast", e);
        }
        
        long processed = messagesProcessed.get();
        long duplicates = duplicatesDetected.get();
        double duplicateRate = processed > 0 ? (double) duplicates / processed : 0.0;
        
        return MetricsSnapshot.builder()
            .messagesProcessed(processed)
            .duplicatesDetected(duplicates)
            .duplicateRate(duplicateRate)
            .cacheSize(cacheSize)
            .cacheHits(cacheHits.get())
            .cacheTotalSize(cacheTotalSize)
            .cacheLocalPartitionSize(cacheLocalPartitionSize)
            .cacheLoadFactor(cacheLoadFactor)
            .clusterSize(clusterSize)
            .checkTimerMean(deduplicationCheckTimer.mean(TimeUnit.MILLISECONDS))
            .checkTimerMax(deduplicationCheckTimer.max(TimeUnit.MILLISECONDS))
            .cacheTimerMean(cacheOperationTimer.mean(TimeUnit.MILLISECONDS))
            .cacheTimerMax(cacheOperationTimer.max(TimeUnit.MILLISECONDS))
            .build();
    }
    
    /**
     * Reset all counters (for testing or monitoring reset).
     */
    public void resetCounters() {
        duplicatesDetected.set(0);
        messagesProcessed.set(0);
        cacheHits.set(0);
        cacheMisses.set(0);
        log.info("Deduplication metrics counters reset");
    }
    
    @lombok.Builder
    @lombok.Data
    public static class MetricsSnapshot {
        private long messagesProcessed;
        private long duplicatesDetected;
        private double duplicateRate;
        private int cacheSize;
        private long cacheHits;
        private int cacheTotalSize;
        private int cacheLocalPartitionSize;
        private double cacheLoadFactor;
        private int clusterSize;
        private double checkTimerMean;
        private double checkTimerMax;
        private double cacheTimerMean;
        private double cacheTimerMax;
        
        /**
         * Calculate cache hit rate.
         */
        public double getCacheHitRate() {
            return cacheHits > 0 ? (double) cacheHits / messagesProcessed : 0.0;
        }
        
        /**
         * Calculate cache efficiency (hits vs total operations).
         */
        public double getCacheEfficiency() {
            long totalCacheOps = cacheHits + (messagesProcessed - duplicatesDetected);
            return totalCacheOps > 0 ? (double) cacheHits / totalCacheOps : 0.0;
        }
    }
}