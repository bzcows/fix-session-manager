package com.fixgateway.component;

import com.fixgateway.model.MessageEnvelope;
import com.fixgateway.service.MessageDeduplicationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

/**
 * Camel processor for message deduplication.
 * Integrates with MessageDeduplicationService to prevent duplicate processing.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MessageDeduplicationProcessor implements Processor {

    private final MessageDeduplicationService deduplicationService;
    private final com.fixgateway.metrics.DeduplicationMetrics deduplicationMetrics;

    @Override
    public void process(Exchange exchange) throws Exception {
        long startTime = System.nanoTime();
        MessageEnvelope envelope = exchange.getIn().getBody(MessageEnvelope.class);
        
        if (envelope == null) {
            log.warn("No MessageEnvelope found in exchange, skipping deduplication");
            return;
        }
        
        // Check if message is a duplicate
        boolean shouldProcess = deduplicationService.processWithDeduplication(envelope);
        
        if (!shouldProcess) {
            // Mark as duplicate and stop processing
            log.info("Duplicate message detected and skipped: messageId={}, session={}, msgType={}, fingerprint={}",
                    envelope.getMessageId(), envelope.getSessionId(), envelope.getMsgType(),
                    envelope.getMessageFingerprint());
            
            // Set header to indicate duplicate
            exchange.getIn().setHeader("X-Duplicate-Message", true);
            exchange.getIn().setHeader("X-Duplicate-Fingerprint", envelope.getMessageFingerprint());
            
            // Record metrics for duplicate
            long duration = System.nanoTime() - startTime;
            deduplicationMetrics.recordDeduplicationCheck(duration, java.util.concurrent.TimeUnit.NANOSECONDS, true);
            
            // Stop further processing by throwing a special exception
            // Camel will handle this in the error handler
            throw new DuplicateMessageException(
                    String.format("Duplicate message detected: %s (session: %s)",
                            envelope.getMessageId(), envelope.getSessionId()));
        }
        
        // Not a duplicate, continue processing
        log.debug("Message passed deduplication check: messageId={}, fingerprint={}",
                envelope.getMessageId(), envelope.getMessageFingerprint());
        exchange.getIn().setHeader("X-Duplicate-Message", false);
        
        // Record metrics for non-duplicate
        long duration = System.nanoTime() - startTime;
        deduplicationMetrics.recordDeduplicationCheck(duration, java.util.concurrent.TimeUnit.NANOSECONDS, false);
    }
    
    /**
     * Exception thrown when a duplicate message is detected.
     * This allows Camel error handlers to handle duplicates specially.
     */
    public static class DuplicateMessageException extends RuntimeException {
        public DuplicateMessageException(String message) {
            super(message);
        }
        
        public DuplicateMessageException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}