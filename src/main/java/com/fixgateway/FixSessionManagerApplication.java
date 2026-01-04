package com.fixgateway;

import com.fixgateway.config.FixLoggingProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(FixLoggingProperties.class)
public class FixSessionManagerApplication {
    public static void main(String[] args) {
        SpringApplication.run(FixSessionManagerApplication.class, args);
    }
}
