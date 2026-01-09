package com.fixgateway.component;

import com.fixgateway.model.MessageEnvelope;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Processor that enriches messages with exception details before sending to DLQ.
 * Extracts exception information from Camel exchange and adds it to MessageEnvelope.
 */
@Slf4j
@Component
public class DlqEnrichmentProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        try {
            // Get the exception that caused the failure
            Exception exception = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
            if (exception == null) {
                exception = exchange.getIn().getHeader(Exchange.EXCEPTION_CAUGHT, Exception.class);
            }
            
            // Get the message body
            Object body = exchange.getIn().getBody();
            
            // Only enrich if we have both an exception and a MessageEnvelope
            if (body instanceof MessageEnvelope && exception != null) {
                MessageEnvelope original = (MessageEnvelope) body;
                
                // Create enriched envelope with error details
                MessageEnvelope enriched = MessageEnvelope.builder()
                    .sessionId(original.getSessionId())
                    .senderCompId(original.getSenderCompId())
                    .targetCompId(original.getTargetCompId())
                    .msgType(original.getMsgType())
                    .clOrdID(original.getClOrdID())
                    .createdTimestamp(original.getCreatedTimestamp())
                    .rawMessage(original.getRawMessage())
                    .errorMessage(exception.getMessage())
                    .errorType(exception.getClass().getName())
                    .errorTimestamp(Instant.now())
                    .errorRouteId(exchange.getFromRouteId())
                    .build();
                
                exchange.getIn().setBody(enriched);
                log.debug("Enriched MessageEnvelope with error details: errorType={}, errorMessage={}", 
                         exception.getClass().getSimpleName(), exception.getMessage());
            } else if (exception != null) {
                log.debug("Cannot enrich non-MessageEnvelope body with error details. Body type: {}", 
                         body != null ? body.getClass().getName() : "null");
            }
        } catch (Exception e) {
            // Don't throw exceptions from the enrichment processor to avoid infinite loops
            log.error("Error in DlqEnrichmentProcessor", e);
        }
    }
}