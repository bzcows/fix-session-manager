package com.fixgateway.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class JacksonConfig {

    /**
     * Provides an ObjectMapper with Java 8 time support for Camel's Jackson data format.
     * This ObjectMapper will be used by Camel when unmarshalling JSON with Instant fields.
     */
    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        // Disable writing dates as timestamps to use ISO-8601 format
        mapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }

    /**
     * Provides a JacksonDataFormat configured with the JSR-310 enabled ObjectMapper
     * specifically for MessageEnvelope class.
     * This can be used in Camel routes for JSON unmarshalling.
     */
    @Bean(name = "jsr310JacksonDataFormat")
    public JacksonDataFormat jsr310JacksonDataFormat(ObjectMapper objectMapper) {
        return new JacksonDataFormat(objectMapper, com.fixgateway.model.MessageEnvelope.class);
    }
}