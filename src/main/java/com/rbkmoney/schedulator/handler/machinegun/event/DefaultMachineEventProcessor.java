package com.rbkmoney.schedulator.handler.machinegun.event;

import com.rbkmoney.damsel.schedule.ScheduleChange;
import com.rbkmoney.machinarium.domain.SignalResultData;
import com.rbkmoney.machinarium.domain.TMachine;
import com.rbkmoney.machinarium.domain.TMachineEvent;
import com.rbkmoney.schedulator.handler.machinegun.MachineEventHandleException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class DefaultMachineEventProcessor implements MachineEventProcessor {

    private final List<MachineEventHandler> machineEventHandlers;

    @Override
    public SignalResultData<ScheduleChange> process(TMachine<ScheduleChange> machine,
                                                    TMachineEvent<ScheduleChange> machineEvent) {
        for (MachineEventHandler machineEventHandler : machineEventHandlers) {
            if (machineEventHandler.isHandle(machine, machineEvent)) {
                return machineEventHandler.handleEvent(machine, machineEvent);
            }
        }
        throw new MachineEventHandleException(String.format("Not found handler for event '%s'", machineEvent));
    }
}
