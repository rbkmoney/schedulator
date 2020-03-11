package com.rbkmoney.schedulator.handler.machinegun.event;

import com.rbkmoney.damsel.schedule.ScheduleChange;
import com.rbkmoney.damsel.schedule.ScheduleJobDeregistered;
import com.rbkmoney.machinarium.domain.SignalResultData;
import com.rbkmoney.machinarium.domain.TMachine;
import com.rbkmoney.machinarium.domain.TMachineEvent;
import com.rbkmoney.machinegun.msgpack.Nil;
import com.rbkmoney.machinegun.msgpack.Value;
import com.rbkmoney.machinegun.stateproc.ComplexAction;
import com.rbkmoney.schedulator.util.TimerActionHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class DeregisterMachineEventHandler implements MachineEventHandler {
    @Override
    public SignalResultData<ScheduleChange> handleEvent(TMachine<ScheduleChange> machine,
                                                        TMachineEvent<ScheduleChange> event,
                                                        DefaultMachineEventChain filterChain) {
        if (!event.getData().isSetScheduleJobDeregistered()) {
            return filterChain.processEventChain(machine, event);
        }
        log.info("Process job deregister event for machineId: {}", machine.getMachineId());
        ComplexAction removeAction = TimerActionHelper.buildRemoveAction();

        ScheduleChange scheduleJobDeregistered = ScheduleChange.schedule_job_deregistered(new ScheduleJobDeregistered());

        return new SignalResultData<>(Value.nl(new Nil()), List.of(scheduleJobDeregistered), removeAction);
    }
}
