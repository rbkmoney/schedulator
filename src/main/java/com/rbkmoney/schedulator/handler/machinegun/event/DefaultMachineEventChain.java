package com.rbkmoney.schedulator.handler.machinegun.event;

import com.rbkmoney.damsel.schedule.ScheduleChange;
import com.rbkmoney.machinarium.domain.SignalResultData;
import com.rbkmoney.machinarium.domain.TMachine;
import com.rbkmoney.machinarium.domain.TMachineEvent;
import com.rbkmoney.schedulator.handler.machinegun.MachineEventHandleException;

import java.util.Iterator;
import java.util.List;

public class DefaultMachineEventChain implements MachineEventChain {

    private Iterator<MachineEventHandler> iterator;

    public DefaultMachineEventChain(List<MachineEventHandler> machineEventHandlers) {
        this.iterator = machineEventHandlers.iterator();
    }

    @Override
    public SignalResultData<ScheduleChange> processEventChain(TMachine<ScheduleChange> machine,
                                                              TMachineEvent<ScheduleChange> event) {
        if (iterator.hasNext()) {
            MachineEventHandler filter = iterator.next();
            return filter.handleEvent(machine, event, this);
        }
        throw new MachineEventHandleException(String.format("Empty next event handler. Event '%s'", machine));
    }
}
