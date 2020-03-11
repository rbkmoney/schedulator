package com.rbkmoney.schedulator.handler.machinegun.event;

import com.rbkmoney.damsel.schedule.ScheduleChange;
import com.rbkmoney.machinarium.domain.SignalResultData;
import com.rbkmoney.machinarium.domain.TMachine;
import com.rbkmoney.machinarium.domain.TMachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class DefaultMachineEventProcessor implements MachineEventProcessor {

    private final List<MachineEventHandler> machineEventHandlers;

    @Override
    public SignalResultData<ScheduleChange> process(TMachine<ScheduleChange> machine, TMachineEvent<ScheduleChange> machineEvent) {
        DefaultMachineEventChain chain = new DefaultMachineEventChain(machineEventHandlers);
        return chain.processEventChain(machine, machineEvent);
    }
}
