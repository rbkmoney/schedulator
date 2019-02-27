package com.rbkmoney.schedulator.config;

import com.rbkmoney.damsel.schedule.ScheduleChange;
import com.rbkmoney.machinarium.client.AutomatonClient;
import com.rbkmoney.machinarium.client.EventSinkClient;
import com.rbkmoney.machinarium.client.TBaseAutomatonClient;
import com.rbkmoney.machinarium.client.TBaseEventSinkClient;
import com.rbkmoney.machinegun.stateproc.AutomatonSrv;
import com.rbkmoney.machinegun.stateproc.EventSinkSrv;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import java.io.IOException;

@Configuration
public class MgConfig {

    @Bean
    public AutomatonSrv.Iface automationThriftClient(
            @Value("${service.mg.automaton.url}") Resource resource,
            @Value("${service.mg.networkTimeout}") int networkTimeout
    ) throws IOException {
        return new THSpawnClientBuilder()
                .withAddress(resource.getURI())
                .withNetworkTimeout(networkTimeout)
                .build(AutomatonSrv.Iface.class);
    }

    @Bean
    public AutomatonClient<ScheduleChange, ScheduleChange> automatonClient(
            @Value("${service.mg.automaton.namespace}") String namespace,
            AutomatonSrv.Iface automationThriftClient
    ) {
        return new TBaseAutomatonClient<>(automationThriftClient, namespace, ScheduleChange.class);
    }

    @Bean
    public EventSinkSrv.Iface eventSinkThriftClient(
            @Value("${service.mg.eventSink.url}") Resource resource,
            @Value("${service.mg.networkTimeout}") int networkTimeout
    ) throws IOException {
        return new THSpawnClientBuilder()
                .withAddress(resource.getURI())
                .withNetworkTimeout(networkTimeout)
                .build(EventSinkSrv.Iface.class);
    }

    @Bean
    public EventSinkClient<ScheduleChange> eventSinkClient(
            @Value("${service.mg.eventSink.sinkId}") String eventSinkId,
            EventSinkSrv.Iface eventSinkThriftClient
    ) {
        return new TBaseEventSinkClient<>(eventSinkThriftClient, eventSinkId, ScheduleChange.class);
    }
}
