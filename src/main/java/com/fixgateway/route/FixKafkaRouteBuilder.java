package com.fixgateway.route;

import com.fixgateway.model.FixSessionConfig;
import com.fixgateway.model.FixSessionsProperties;
import com.fixgateway.model.MessageEnvelope;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
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
    private final SessionOwnershipService ownershipService;
    private final CamelContext camelContext;
    
    @Value("${kafka.bootstrap.servers:localhost:9092}")
    private String kafkaBootstrapServers;

    @Override
    public void configure() throws Exception {
        // Register for ownership events to control Kafka routes
        ownershipService.registerListener(new SessionOwnershipService.OwnershipListener() {
            @Override
            public void onOwnershipAcquired(String sessionKey) {
                startRoutes(sessionKey);
            }

            @Override
            public void onOwnershipLost(String sessionKey) {
                stopRoutes(sessionKey);
            }
        });
        
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
            .logExhausted(true));

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

    private void startRoutes(String sessionKey) {
        try {
            camelContext.getRouteController().startRoute("fix-to-kafka-" + sessionKey);
            camelContext.getRouteController().startRoute("kafka-to-fix-" + sessionKey);
            log.info("Kafka routes started for FIX session {}", sessionKey);
        } catch (Exception e) {
            log.error("Failed to start Kafka routes for {}", sessionKey, e);
        }
    }

    private void stopRoutes(String sessionKey) {
        try {
            camelContext.getRouteController().stopRoute("fix-to-kafka-" + sessionKey);
            camelContext.getRouteController().stopRoute("kafka-to-fix-" + sessionKey);
            log.info("Kafka routes stopped for FIX session {}", sessionKey);
        } catch (Exception e) {
            log.error("Failed to stop Kafka routes for {}", sessionKey, e);
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
            .log(LoggingLevel.DEBUG, "Publishing inbound FIX message to Kafka topic: " + inputTopic)
            .to(kafkaProducerUri);

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
        
        // Construct Kafka URI with correct parameters for Camel 4.14.0
        // DIAGNOSTIC: Explicitly disable transactional features to prevent FindCoordinator NPE
        // The error "Cannot invoke String.equals(Object) because this.key is null" occurs when
        // Kafka tries to find a transaction coordinator but transactional.id is not set
        
        String kafkaUri = "kafka:" + outputTopic +
             "?brokers=" + brokers +
             "&groupId=fix-gateway-" + sessionKey +
             "&autoOffsetReset=earliest" +
             "&maxPollRecords=100" +
             "&keyDeserializer=org.apache.kafka.common.serialization.StringDeserializer" +
             "&valueDeserializer=org.apache.kafka.common.serialization.StringDeserializer" +
             "&consumersCount=1" +
             // DIAGNOSTIC: Explicitly disable producer idempotent features
             // Setting enable.idempotence=false prevents the FindCoordinator NPE
             "&additionalProperties.enable.idempotence=false" +
             "&additionalProperties.acks=1";
        
        log.info("DIAGNOSTIC: Kafka consumer URI with idempotence disabled: {}", kafkaUri);
        
        from(kafkaUri)
            .routeId(routeId)
            .autoStartup(false)
            .log(LoggingLevel.INFO, "Received message from Kafka topic: " + outputTopic)
            .unmarshal().json(JsonLibrary.Jackson, MessageEnvelope.class)
            .process(exchange -> {
                MessageEnvelope envelope = exchange.getIn().getBody(MessageEnvelope.class);
                String rawMessage = envelope.getRawMessage();
                
                // Parse and send FIX message
                SessionID sessionID = new SessionID(
                    config.getFixVersion(),
                    config.getSenderCompId(),
                    config.getTargetCompId()
                );
                
                Session session = Session.lookupSession(sessionID);
                if (session == null) {
                    throw new IllegalStateException("FIX session not found: " + sessionID);
                }
                
                if (!session.isLoggedOn()) {
                    throw new IllegalStateException("FIX session not logged on: " + sessionID);
                }
                
                Message message = new Message(rawMessage);
                session.send(message);
                
                log.info("Sent FIX message to session {}: {}", sessionID, envelope.getMsgType());
            })
            .log(LoggingLevel.INFO, "Successfully forwarded message to FIX session");
    }
}
