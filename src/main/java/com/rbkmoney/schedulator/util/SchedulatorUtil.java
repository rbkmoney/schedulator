package com.rbkmoney.schedulator.util;

import com.rbkmoney.damsel.schedule.Event;
import com.rbkmoney.damsel.schedule.EventPayload;
import com.rbkmoney.damsel.schedule.ScheduleChange;
import com.rbkmoney.machinarium.domain.TSinkEvent;

import java.util.Collections;

public class SchedulatorUtil {
    public static Event toEvent(TSinkEvent<ScheduleChange> changeTSinkEvent) {
        return new Event(
                changeTSinkEvent.getEvent().getId(),
                changeTSinkEvent.getEvent().getCreatedAt().toString(),
                changeTSinkEvent.getSourceId(),
                new EventPayload(Collections.singletonList(changeTSinkEvent.getEvent().getData()))
        );
    }
}
