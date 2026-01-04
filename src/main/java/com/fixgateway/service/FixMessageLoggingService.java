package com.fixgateway.service;

import com.fixgateway.config.FixLoggingProperties;
import com.fixgateway.model.FixMessageLog;
import com.fixgateway.repository.FixMessageRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import quickfix.Message;
import quickfix.SessionID;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "fix.logging.enabled", havingValue = "true", matchIfMissing = true)
public class FixMessageLoggingService implements FixMessageLogger {
    
    private final FixMessageRepository repository;
    private final FixLoggingProperties properties;
    private final CoordinatorService coordinatorService;
    private final com.fixgateway.metrics.LoggingMetrics loggingMetrics;
    
    private BlockingQueue<FixMessageLog> messageQueue;
    private ScheduledExecutorService executorService;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    
    // Simple circuit breaker state
    private final AtomicBoolean circuitOpen = new AtomicBoolean(false);
    private final AtomicLong circuitOpenTime = new AtomicLong(0);
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    
    private final AtomicLong loggedCount = new AtomicLong(0);
    private final AtomicLong failedCount = new AtomicLong(0);
    private final AtomicLong droppedCount = new AtomicLong(0);
    
    @PostConstruct
    public void init() {
        if (!properties.isEnabled()) {
            log.info("FixMessageLoggingService disabled via configuration");
            return;
        }
        
        // Initialize queue
        messageQueue = new LinkedBlockingQueue<>(properties.getAsync().getQueueCapacity());
        
        // Initialize executor service
        executorService = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
            r -> {
                Thread t = new Thread(r, "fix-message-logger");
                t.setDaemon(true);
                return t;
            }
        );
        
        // Start background writer
        startBackgroundWriter();
        
        // Start metrics updater
        startMetricsUpdater();
        
        log.info("FixMessageLoggingService initialized with queue capacity: {}, batch size: {}, flush interval: {}ms",
                 properties.getAsync().getQueueCapacity(),
                 properties.getAsync().getBatchSize(),
                 properties.getAsync().getFlushIntervalMs());
    }
    
    @PreDestroy
    public void shutdown() {
        isRunning.set(false);
        
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                executorService.shutdownNow();
            }
        }
        
        // Flush remaining messages
        flush();
        
        log.info("FixMessageLoggingService shutdown complete. Stats: logged={}, failed={}, dropped={}",
                 loggedCount.get(), failedCount.get(), droppedCount.get());
    }
    
    @Override
    public void logInboundApp(SessionID sessionId, Message message, Instant timestamp) {
        logMessage(sessionId, message, timestamp, FixMessageLog.MessageDirection.INBOUND, 
                  FixMessageLog.MessageCategory.APPLICATION);
    }
    
    @Override
    public void logOutboundApp(SessionID sessionId, Message message, Instant timestamp) {
        logMessage(sessionId, message, timestamp, FixMessageLog.MessageDirection.OUTBOUND, 
                  FixMessageLog.MessageCategory.APPLICATION);
    }
    
    @Override
    public void logInboundAdmin(SessionID sessionId, Message message, Instant timestamp) {
        logMessage(sessionId, message, timestamp, FixMessageLog.MessageDirection.INBOUND, 
                  FixMessageLog.MessageCategory.ADMIN);
    }
    
    @Override
    public void logOutboundAdmin(SessionID sessionId, Message message, Instant timestamp) {
        logMessage(sessionId, message, timestamp, FixMessageLog.MessageDirection.OUTBOUND, 
                  FixMessageLog.MessageCategory.ADMIN);
    }
    
    private void logMessage(SessionID sessionId, Message message, Instant timestamp,
                           FixMessageLog.MessageDirection direction, FixMessageLog.MessageCategory category) {
        if (!properties.isEnabled()) {
            return;
        }
        
        try {
            FixMessageLog logEntry = FixMessageLog.fromMessage(
                message, sessionId, direction, category, timestamp, coordinatorService.localNodeId());
            
            boolean offered = messageQueue.offer(logEntry);
            
            if (!offered) {
                // Queue full - apply backpressure strategy
                handleQueueFull(logEntry);
            }
        } catch (Exception e) {
            log.error("Error creating log entry for session: {}", sessionId, e);
            failedCount.incrementAndGet();
        }
    }
    
    private void handleQueueFull(FixMessageLog logEntry) {
        FixLoggingProperties.BackpressureStrategy strategy = 
            properties.getAsync().getBackpressureStrategy();
        
        switch (strategy) {
            case DROP_OLDEST:
                // Remove oldest and add new
                messageQueue.poll();
                boolean offered = messageQueue.offer(logEntry);
                if (!offered) {
                    // Should not happen, but just in case
                    droppedCount.incrementAndGet();
                    if (loggingMetrics != null) {
                        loggingMetrics.incrementDroppedCount(1);
                    }
                    log.warn("Failed to add message after dropping oldest, queue still full");
                } else {
                    droppedCount.incrementAndGet();
                    if (loggingMetrics != null) {
                        loggingMetrics.incrementDroppedCount(1);
                    }
                    log.debug("Dropped oldest message to make room for new message");
                }
                break;
                
            case DROP_NEWEST:
                droppedCount.incrementAndGet();
                if (loggingMetrics != null) {
                    loggingMetrics.incrementDroppedCount(1);
                }
                log.debug("Dropped new message due to queue full");
                break;
                
            case BLOCK:
                try {
                    messageQueue.put(logEntry); // Block until space available
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    droppedCount.incrementAndGet();
                    log.warn("Interrupted while waiting to add message to queue");
                }
                break;
                
            case SYNCHRONOUS_FALLBACK:
                // Write synchronously (slower but ensures no loss)
                writeSynchronously(logEntry);
                break;
                
            default:
                droppedCount.incrementAndGet();
                log.warn("Unknown backpressure strategy: {}, dropping message", strategy);
        }
    }
    
    private void writeSynchronously(FixMessageLog logEntry) {
        try {
            repository.save(logEntry);
            loggedCount.incrementAndGet();
        } catch (Exception e) {
            failedCount.incrementAndGet();
            log.error("Failed to write message synchronously", e);
        }
    }
    
    private void startBackgroundWriter() {
        executorService.scheduleAtFixedRate(
            this::processBatch,
            properties.getAsync().getFlushIntervalMs(),
            properties.getAsync().getFlushIntervalMs(),
            TimeUnit.MILLISECONDS
        );
    }
    
    private void startMetricsUpdater() {
        executorService.scheduleAtFixedRate(
            this::updateMetrics,
            5,  // Initial delay
            5,  // Update every 5 seconds
            TimeUnit.SECONDS
        );
    }
    
    private void updateMetrics() {
        if (loggingMetrics != null) {
            loggingMetrics.updateQueueSize(getQueueSize());
            loggingMetrics.updateQueueUtilization((int) getQueueUtilization());
            loggingMetrics.updateCircuitBreakerState(getCircuitBreakerState());
        }
    }
    
    private void processBatch() {
        if (!isRunning.get() || messageQueue.isEmpty()) {
            return;
        }
        
        List<FixMessageLog> batch = new ArrayList<>();
        messageQueue.drainTo(batch, properties.getAsync().getBatchSize());
        
        if (!batch.isEmpty()) {
            writeBatchToDatabase(batch);
        }
    }
    
    private void writeBatchToDatabase(List<FixMessageLog> batch) {
        if (!properties.getCircuitBreaker().isEnabled()) {
            writeBatchDirectly(batch);
            return;
        }
        
        // Check circuit breaker state
        if (isCircuitOpen()) {
            log.warn("Circuit breaker is OPEN, dropping batch of {} messages", batch.size());
            handleBatchFailure(batch, new RuntimeException("Circuit breaker is OPEN"));
            return;
        }
        
        try {
            writeBatchDirectly(batch);
            // Success - reset failure count
            consecutiveFailures.set(0);
        } catch (Exception e) {
            log.error("Failed to write batch of {} messages to database", batch.size(), e);
            handleBatchFailure(batch, e);
        }
    }
    
    private void writeBatchDirectly(List<FixMessageLog> batch) {
        long startTime = System.nanoTime();
        try {
            repository.saveAll(batch);
            loggedCount.addAndGet(batch.size());
            if (loggingMetrics != null) {
                loggingMetrics.incrementLoggedCount(batch.size());
                long duration = System.nanoTime() - startTime;
                loggingMetrics.recordWriteDuration(duration, TimeUnit.NANOSECONDS);
            }
            log.debug("Successfully persisted {} messages to database", batch.size());
        } catch (Exception e) {
            log.error("Failed to persist batch of {} messages to database", batch.size(), e);
            failedCount.addAndGet(batch.size());
            if (loggingMetrics != null) {
                loggingMetrics.incrementFailedCount(batch.size());
            }
            handleBatchFailure(batch, e);
        }
    }
    
    private void handleBatchFailure(List<FixMessageLog> batch, Exception e) {
        // Record failure for circuit breaker
        recordFailure();
        
        // For now, we just log the error and drop the messages.
        // In a production system, you might want to:
        // 1. Retry with exponential backoff
        // 2. Write to a dead letter queue
        // 3. Alert operators
        
        log.error("Dropping {} messages due to database failure: {}", batch.size(), e.getMessage());
        
        // Simple retry logic (could be enhanced)
        if (properties.getAsync().getMaxRetries() > 0) {
            // For simplicity, we're not implementing full retry logic here
            // In production, you'd want to implement proper retry with backoff
        }
    }
    
    @Override
    public boolean isEnabled() {
        return properties.isEnabled();
    }
    
    @Override
    public int getQueueSize() {
        return messageQueue != null ? messageQueue.size() : 0;
    }
    
    @Override
    public long getLoggedCount() {
        return loggedCount.get();
    }
    
    @Override
    public long getFailedCount() {
        return failedCount.get();
    }
    
    @Override
    public void flush() {
        if (messageQueue == null || messageQueue.isEmpty()) {
            return;
        }
        
        List<FixMessageLog> remaining = new ArrayList<>();
        messageQueue.drainTo(remaining);
        
        if (!remaining.isEmpty()) {
            log.info("Flushing {} remaining messages to database", remaining.size());
            writeBatchToDatabase(remaining);
        }
    }
    
    private boolean isCircuitOpen() {
        if (!circuitOpen.get()) {
            return false;
        }
        
        // Check if we should transition to half-open
        long openDuration = System.currentTimeMillis() - circuitOpenTime.get();
        if (openDuration > properties.getCircuitBreaker().getHalfOpenTimeoutMs()) {
            // Transition to half-open
            circuitOpen.set(false);
            log.info("Circuit breaker transitioned to HALF_OPEN after {}ms", openDuration);
            return false;
        }
        
        return true;
    }
    
    private void recordFailure() {
        int failures = consecutiveFailures.incrementAndGet();
        if (failures >= properties.getCircuitBreaker().getFailureThreshold() && !circuitOpen.get()) {
            circuitOpen.set(true);
            circuitOpenTime.set(System.currentTimeMillis());
            log.warn("Circuit breaker OPENED after {} consecutive failures", failures);
        }
    }
    
    @Override
    public String getCircuitBreakerState() {
        if (!properties.getCircuitBreaker().isEnabled()) {
            return "DISABLED";
        }
        
        if (circuitOpen.get()) {
            long openDuration = System.currentTimeMillis() - circuitOpenTime.get();
            if (openDuration > properties.getCircuitBreaker().getHalfOpenTimeoutMs()) {
                return "HALF_OPEN";
            }
            return "OPEN";
        }
        
        return "CLOSED";
    }
    
    /**
     * Get number of dropped messages
     */
    public long getDroppedCount() {
        return droppedCount.get();
    }
    
    /**
     * Get queue capacity
     */
    public int getQueueCapacity() {
        return properties.getAsync().getQueueCapacity();
    }
    
    /**
     * Get queue utilization percentage
     */
    public double getQueueUtilization() {
        if (messageQueue == null) {
            return 0.0;
        }
        return (double) messageQueue.size() / properties.getAsync().getQueueCapacity() * 100.0;
    }
}