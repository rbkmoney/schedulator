package com.rbkmoney.schedulator.handler;

import com.rbkmoney.machinarium.client.EventSinkClient;
import com.rbkmoney.machinarium.domain.TSinkEvent;
import com.rbkmoney.damsel.schedule.*;
import com.rbkmoney.schedulator.util.SchedulatorUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class EventSinkHandler implements EventSinkSrv.Iface {

    private final EventSinkClient<ScheduleChange> eventSinkClient;

    public EventSinkHandler(EventSinkClient<ScheduleChange> eventSinkClient) {
        this.eventSinkClient = eventSinkClient;
    }

    @Override
    public List<Event> getEvents(EventRange eventRange) throws TException {
        List<TSinkEvent<ScheduleChange>> events;
        if (eventRange.isSetAfter()) {
            events = eventSinkClient.getEvents(eventRange.getLimit(), eventRange.getAfter());
        } else {
            events = eventSinkClient.getEvents(eventRange.getLimit());
        }

        return events.stream()
                .map(SchedulatorUtil::toEvent)
                .collect(Collectors.toList());
    }

    @Override
    public long getLastEventID() throws NoLastEvent, TException {
        return eventSinkClient.getLastEventId().orElse(Long.MIN_VALUE);
    }

}
