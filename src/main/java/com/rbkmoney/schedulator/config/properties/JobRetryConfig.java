package com.rbkmoney.schedulator.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties("retry-policy")
@Data
public class JobRetryConfig {

    private JobPolicy job;

    @Data
    public static final class JobPolicy {

        private Integer initialIntervalSeconds;

        private Integer maxIntervalSeconds;

        private Integer maxAttempts;

    }

}
