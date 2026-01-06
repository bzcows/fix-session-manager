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
        
        Long lastOffset = lastProcessedMap.get(mapKey);
        
        if (lastOffset != null && currentOffset <= lastOffset) {
            log.error("OUT OF ORDER DETECTED: Session={}, CurrentOffset={}, LastOffset={}, Topic={}", 
                     sessionKey, currentOffset, lastOffset, 
                     exchange.getIn().getHeader("kafka.TOPIC", String.class));
            throw new IllegalStateException(
                String.format("Message ordering violation detected for session %s. " +
                            "Current offset %d is not greater than last offset %d", 
                            sessionKey, currentOffset, lastOffset));
        }
        
        // Update last processed offset
        lastProcessedMap.put(mapKey, currentOffset);
        log.debug("Updated last processed offset for session {}: {}", sessionKey, currentOffset);
    }
}