package com.fixgateway.route;

import com.fixgateway.component.DlqEnrichmentProcessor;
import com.fixgateway.component.MessageOrderValidator;
import com.fixgateway.model.FixSessionConfig;
import com.fixgateway.model.FixSessionsProperties;
import com.fixgateway.model.MessageEnvelope;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import quickfix.Message;
import quickfix.Session;
import quickfix.SessionID;
import com.fixgateway.service.SessionOwnershipService;
import org.apache.camel.CamelContext;

@Slf4j
@Component
@RequiredArgsConstructor
public class FixKafkaRouteBuilder extends RouteBuilder {

    private final FixSessionsProperties sessionsProperties;
    private final CamelContext camelContext;
    private final JacksonDataFormat jsr310JacksonDataFormat;
    private final DlqEnrichmentProcessor dlqEnrichmentProcessor;
    private final MessageOrderValidator messageOrderValidator;
    
    @Value("${kafka.bootstrap.servers:localhost:9092}")
    private String kafkaBootstrapServers;

    @Override
    public void configure() throws Exception {
        // Get brokers for DLQ configuration
        String brokers = kafkaBootstrapServers != null ? kafkaBootstrapServers : "localhost:9092";
        
        // Error handler with dead letter queue
        // DIAGNOSTIC: Disable transactional features on DLQ producer to prevent FindCoordinator NPE
        String dlqUri = "kafka:fix.dlq" +
            "?brokers=" + brokers +
            "&groupId=fix-gateway-dlq" +
            "&autoOffsetReset=earliest" +
            // DIAGNOSTIC: Explicitly disable producer transactional/idempotent features for DLQ
            // Setting enable.idempotence=false prevents the FindCoordinator NPE
            "&additionalProperties.enable.idempotence=false" +
            "&additionalProperties.acks=1";
        
        log.info("DIAGNOSTIC: DLQ URI with idempotence disabled: {}", dlqUri);
        
        errorHandler(deadLetterChannel(dlqUri)
            .maximumRedeliveries(3)
            .redeliveryDelay(1000)
            .useExponentialBackOff()
            .logStackTrace(true)
            .logExhausted(true)
            .onPrepareFailure(dlqEnrichmentProcessor));

        // Create routes for each enabled session but DO NOT auto-start them.
        // Routes will be started only after the corresponding FIX session is fully logged on.
        for (FixSessionConfig config : sessionsProperties.getSessions()) {
            if (!config.isEnabled()) {
                continue;
            }

            String sessionKey = config.getSenderCompId() + "-" + config.getTargetCompId();
            createOutboundRoute(config, sessionKey);
            createInboundRoute(config, sessionKey);
        }
    }


    /**
     * FIX → Kafka (Inbound)
     * Creates fix.<SENDER>.<TARGET>.input topics by producing inbound FIX messages
     */
    private void createInboundRoute(FixSessionConfig config, String sessionKey) {
        String inputTopic = String.format("fix.%s.%s.input",
            config.getSenderCompId(), config.getTargetCompId());

        String routeId = "fix-to-kafka-" + sessionKey;

        String brokers = kafkaBootstrapServers != null ? kafkaBootstrapServers : "localhost:9092";

        log.info("Creating FIX → Kafka inbound route {} for topic {} with brokers {}",
            routeId, inputTopic, brokers);

        String directEndpoint = "direct:fix-inbound-" + sessionKey;

        String kafkaProducerUri = "kafka:" + inputTopic +
            "?brokers=" + brokers +
            "&keySerializer=org.apache.kafka.common.serialization.StringSerializer" +
            "&valueSerializer=org.apache.kafka.common.serialization.StringSerializer" +
            "&additionalProperties.enable.idempotence=false" +
            "&additionalProperties.acks=1" +
            "&additionalProperties.allow.auto.create.topics=true";

        from(directEndpoint)
            .routeId(routeId)
            .autoStartup(false)
            .log(LoggingLevel.INFO, "Received message from Kafka topic: " + inputTopic +": ${body}")
            // Set partition key from header if present (for content-based routing)
            .process(exchange -> {
               /*
                Object partitionKey = exchange.getIn().getHeader(KafkaConstants.KEY);
                Object partition = exchange.getIn().getHeader(KafkaConstants.PARTITION);
                
                if (partitionKey != null) {
                    log.info("DIAGNOSTIC: Using partition key for routing: {} (type: {})", partitionKey,
                        partitionKey.getClass().getSimpleName());
                    
                    // Calculate expected partition for debugging
                    try {
                        String keyString = partitionKey.toString();
                        int hash = keyString.hashCode(); // Java's default hash
                        int expectedPartition = Math.abs(hash) % config.getInputPartitions(); // Use config partitions
                        log.info("DIAGNOSTIC: Key '{}' hash: {}, expected partition: {} (of {})",
                            keyString, hash, expectedPartition, config.getInputPartitions());
                    } catch (Exception e) {
                        log.warn("DIAGNOSTIC: Could not calculate expected partition: {}", e.getMessage());
                    }
                } else {
                    log.info("DIAGNOSTIC: No partition key header (KafkaConstants.KEY) found");
                }
                if (partition != null) {
                    log.info("DIAGNOSTIC: Using explicit partition number: {} (type: {})", partition,
                        partition.getClass().getSimpleName());
                } else {
                    log.info("DIAGNOSTIC: No partition number header (KafkaConstants.PARTITION) found");
                }*/
                
                // Log all headers for debugging
                log.debug("DIAGNOSTIC: All headers: {}", exchange.getIn().getHeaders());
            })
            .to(kafkaProducerUri)
            // Add processor to log the actual partition after sending
            .process(exchange -> {
                // Try to get the partition from Kafka record metadata
                Object kafkaPartition = exchange.getIn().getHeader("kafka.PARTITION");
                Object kafkaOffset = exchange.getIn().getHeader("kafka.OFFSET");
                Object kafkaTopic = exchange.getIn().getHeader("kafka.TOPIC");
                
                if (kafkaPartition != null) {
                    log.info("DIAGNOSTIC: Message sent to Kafka - Topic: {}, Partition: {}, Offset: {}",
                        kafkaTopic, kafkaPartition, kafkaOffset);
                } else {
                    log.info("DIAGNOSTIC: Message sent to Kafka - Topic: {} (partition/offset not available)",
                        kafkaTopic);
                }
            });

        log.info("Inbound FIX messages must be sent to '{}' from FixApplication", directEndpoint);
    }

    private void createOutboundRoute(FixSessionConfig config, String sessionKey) {
        // Kafka → FIX (Outbound)
        String outputTopic = String.format("fix.%s.%s.output",
            config.getSenderCompId(), config.getTargetCompId());
        
        String routeId = "kafka-to-fix-" + sessionKey;
        
        // Debug: check if brokers value is being injected
        log.info("Creating Kafka route {} for topic {} with brokers: {} (injected value: {})",
            routeId, outputTopic, kafkaBootstrapServers,
            kafkaBootstrapServers != null ? kafkaBootstrapServers : "NULL");
        
        // Use brokers parameter for Camel 4.x Kafka component
        String brokers = kafkaBootstrapServers != null ? kafkaBootstrapServers : "localhost:9092";
        
       
        // CRITICAL: Configure for strict message ordering guarantees
        // Process one message at a time to ensure FIFO ordering
        String kafkaUri = "kafka:" + outputTopic +
             "?brokers=" + brokers +
             "&groupId=fix-gateway-" + sessionKey +
             "&autoOffsetReset=earliest" +
             "&maxPollRecords=1" +  // CRITICAL: Process one message at a time
             "&keyDeserializer=org.apache.kafka.common.serialization.StringDeserializer" +
             "&valueDeserializer=org.apache.kafka.common.serialization.StringDeserializer" +
             "&consumersCount=1" +
             // Add ordering guarantees
             "&synchronous=true" +
             "&allowManualCommit=false" +
             "&breakOnFirstError=true" +
             // Disable async processing
             "&additionalProperties.max.poll.interval.ms=300000" +  // 5 minutes
             "&additionalProperties.enable.auto.commit=false" +
             "&additionalProperties.enable.idempotence=false" +
             "&additionalProperties.acks=1";
         
        log.info("STRICT ORDERING: Kafka consumer URI configured for sequential processing: {}", kafkaUri);
         
        from(kafkaUri)
            .routeId(routeId)
            .autoStartup(false)
            // CRITICAL: Single-threaded processing for strict ordering
            .threads(1).maxPoolSize(1)
            .log(LoggingLevel.INFO, "Publish message from Kafka topic: " + outputTopic +": ${body}")
            // Validate message ordering before processing
            .process(messageOrderValidator)
            .unmarshal(jsr310JacksonDataFormat)
            .log(LoggingLevel.DEBUG, "Unmarshalled envelope: ${body}")
            .process(exchange -> {
                MessageEnvelope envelope = exchange.getIn().getBody(MessageEnvelope.class);
                log.info("Processing Kafka→FIX message for session: {}, msgType: {}, timestamp: {}",
                    envelope.getSessionId(), envelope.getMsgType(), envelope.getCreatedTimestamp());
                
                String rawMessage = envelope.getRawMessage();
                if (rawMessage == null || rawMessage.trim().isEmpty()) {
                    throw new IllegalStateException("Raw message is null or empty in envelope");
                }
                
                // Try multiple session lookup strategies
                Session session = null;
                SessionID sessionID = null;
                
                // Strategy 1: Try parsing from envelope sessionId string
                if (envelope.getSessionId() != null && !envelope.getSessionId().isEmpty()) {
                    try {
                        sessionID = new SessionID(envelope.getSessionId());
                        session = Session.lookupSession(sessionID);
                        log.debug("Session lookup by envelope sessionId '{}': {}",
                            envelope.getSessionId(), session != null ? "FOUND" : "NOT FOUND");
                    } catch (Exception e) {
                        log.debug("Cannot parse SessionID from envelope: {}", envelope.getSessionId(), e);
                    }
                }
                
                // Strategy 2: Use config-based SessionID (original approach)
                if (session == null) {
                    sessionID = new SessionID(
                        config.getFixVersion(),
                        config.getSenderCompId(),
                        config.getTargetCompId()
                    );
                    session = Session.lookupSession(sessionID);
                    log.debug("Session lookup by config '{}-{}': {}",
                        config.getSenderCompId(), config.getTargetCompId(),
                        session != null ? "FOUND" : "NOT FOUND");
                }
                
                // Strategy 3: Try with reversed sender/target
                if (session == null) {
                    sessionID = new SessionID(
                        config.getFixVersion(),
                        config.getTargetCompId(),
                        config.getSenderCompId()
                    );
                    session = Session.lookupSession(sessionID);
                    log.debug("Session lookup by swapped '{}-{}': {}",
                        config.getTargetCompId(), config.getSenderCompId(),
                        session != null ? "FOUND" : "NOT FOUND");
                }
                
                if (session == null) {
                    log.error("FIX session not found. Envelope sessionId: '{}'. Config: {}-{}",
                        envelope.getSessionId(), config.getSenderCompId(), config.getTargetCompId());
                    throw new IllegalStateException("FIX session not found: " + envelope.getSessionId());
                }
                
                log.debug("Session found: {}, logged on: {}", sessionID, session.isLoggedOn());
                
                if (!session.isLoggedOn()) {
                    log.error("FIX session found but not logged on: {}", sessionID);
                    throw new IllegalStateException("FIX session not logged on: " + sessionID);
                }
                
                Message message = new Message(rawMessage);
                session.send(message);
                
                log.info("Successfully sent FIX message to session {}: msgType={}", sessionID, envelope.getMsgType());
            })
            .log(LoggingLevel.INFO, "Successfully forwarded message to FIX session");
    }
}
