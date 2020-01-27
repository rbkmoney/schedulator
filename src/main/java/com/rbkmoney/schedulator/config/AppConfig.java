package com.rbkmoney.schedulator.config;

import com.rbkmoney.damsel.domain_config.RepositoryClientSrv;
import com.rbkmoney.schedulator.handler.MgProcessorHandler;
import com.rbkmoney.schedulator.handler.MgProcessorMdcDecorator;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.Resource;

import java.io.IOException;

@Configuration
public class AppConfig {
    @Bean
    public RepositoryClientSrv.Iface dominantClient(@Value("${service.dominant.url}") Resource resource,
                                                    @Value("${service.dominant.networkTimeout}") int networkTimeout) throws IOException {
        return new THSpawnClientBuilder()
                .withNetworkTimeout(networkTimeout)
                .withAddress(resource.getURI()).build(RepositoryClientSrv.Iface.class);
    }

    @Bean
    @Primary
    @Autowired
    public MgProcessorMdcDecorator mgProcessorHandlerDecorator(MgProcessorHandler mgProcessorHandler) {
        return new MgProcessorMdcDecorator(mgProcessorHandler);
    }

}
