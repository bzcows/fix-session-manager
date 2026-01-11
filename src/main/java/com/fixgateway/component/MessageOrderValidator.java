package com.fixgateway.component;

import com.fixgateway.model.MessageEnvelope;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessageOrderValidator implements Processor {
    
    private final HazelcastInstance hazelcastInstance;
    
    @Override
    public void process(Exchange exchange) throws Exception {
        MessageEnvelope envelope = exchange.getIn().getBody(MessageEnvelope.class);
        if (envelope == null) {
            return; // Nothing to validate
        }
        
        String sessionKey = envelope.getSessionId();
        if (sessionKey == null || sessionKey.isEmpty()) {
            // Use sender/target as fallback
            sessionKey = envelope.getSenderCompId() + "-" + envelope.getTargetCompId();
        }
        
        IMap<String, Long> lastProcessedMap = hazelcastInstance.getMap("last-processed-offset");
        String mapKey = "offset:" + sessionKey;
        
        // Extract Kafka offset from headers
        Long currentOffset = exchange.getIn().getHeader("kafka.OFFSET", Long.class);
        if (currentOffset == null) {
            // If no offset header, skip validation but log warning
            log.warn("No Kafka offset header found for session: {}", sessionKey);
            return;
        }
        
        // Use atomic compute operation to prevent race conditions
        // This ensures check-and-update happens atomically in Hazelcast
        // Create final copies for use in lambda
        final String finalSessionKey = sessionKey;
        final Long finalCurrentOffset = currentOffset;
        
        try {
            lastProcessedMap.compute(mapKey, (key, existingOffset) -> {
                if (existingOffset != null && finalCurrentOffset <= existingOffset) {
                    // Throw RuntimeException to be caught and rethrown as IllegalStateException
                    throw new RuntimeException(
                        String.format("Message ordering violation detected for session %s. " +
                                    "Current offset %d is not greater than last offset %d",
                                    finalSessionKey, finalCurrentOffset, existingOffset));
                }
                // Atomically update to current offset
                return finalCurrentOffset;
            });
            log.debug("Atomically updated last processed offset for session {}: {}", finalSessionKey, finalCurrentOffset);
        } catch (RuntimeException e) {
            // Unwrap the RuntimeException thrown in compute()
            log.error("OUT OF ORDER DETECTED: Session={}, CurrentOffset={}, Topic={}",
                     finalSessionKey, finalCurrentOffset,
                     exchange.getIn().getHeader("kafka.TOPIC", String.class));
            throw new IllegalStateException(e.getMessage());
        }
    }
}