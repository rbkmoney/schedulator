package com.rbkmoney.schedulator.handler;

import com.rbkmoney.damsel.schedule.ScheduleChange;
import com.rbkmoney.damsel.schedule.ScheduleJobDeregistered;
import com.rbkmoney.machinarium.domain.SignalResultData;
import com.rbkmoney.machinarium.domain.TMachineEvent;
import com.rbkmoney.machinegun.msgpack.Nil;
import com.rbkmoney.machinegun.msgpack.Value;
import com.rbkmoney.machinegun.stateproc.ComplexAction;
import com.rbkmoney.schedulator.util.TimerActionHelper;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class RemoveMachineEventHandler extends BaseMachineEventHandler<ScheduleChange> {

    protected RemoveMachineEventHandler() {
        super(null);
    }

    @Override
    protected SignalResultData<ScheduleChange> handleEvent(TMachineEvent<ScheduleChange> machineEvent) {
        log.info("Handle remove schedule machine event");
        ComplexAction removeAction = TimerActionHelper.buildRemoveAction();

        ScheduleChange scheduleJobDeregistered = ScheduleChange.schedule_job_deregistered(new ScheduleJobDeregistered());

        return new SignalResultData<>(Value.nl(new Nil()), List.of(scheduleJobDeregistered), removeAction);
    }

    @Override
    public boolean canHandle(TMachineEvent<ScheduleChange> machineEvent) {
        return machineEvent.getData().isSetScheduleJobDeregistered();
    }

}
