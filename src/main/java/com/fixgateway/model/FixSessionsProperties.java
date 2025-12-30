package com.fixgateway.model;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Data
@Component
@ConfigurationProperties(prefix = "fix")
public class FixSessionsProperties {
    private List<FixSessionConfig> sessions = new ArrayList<>();
}
