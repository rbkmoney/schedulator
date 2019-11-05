package com.rbkmoney.schedulator.handler;

import com.rbkmoney.damsel.schedule.ScheduledJobExecutorSrv;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import org.springframework.stereotype.Component;

import java.net.URI;

@Component
public class RemoteClientManager {

    public ScheduledJobExecutorSrv.Iface getRemoteClient(String url) {
        return new THSpawnClientBuilder()
                .withAddress(URI.create(url))
                .build(ScheduledJobExecutorSrv.Iface.class);
    }

}
