package com.rbkmoney.schedulator.config;

import com.rbkmoney.schedulator.handler.machinegun.MgProcessorHandler;
import com.rbkmoney.schedulator.handler.machinegun.MgProcessorMdcDecorator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class AppConfig {

    @Bean
    @Primary
    @Autowired
    public MgProcessorMdcDecorator mgProcessorHandlerDecorator(MgProcessorHandler mgProcessorHandler) {
        return new MgProcessorMdcDecorator(mgProcessorHandler);
    }

}
