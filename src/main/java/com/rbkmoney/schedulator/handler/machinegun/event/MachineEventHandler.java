package com.rbkmoney.schedulator.handler.machinegun.event;

import com.rbkmoney.damsel.schedule.ScheduleChange;
import com.rbkmoney.machinarium.domain.SignalResultData;
import com.rbkmoney.machinarium.domain.TMachine;
import com.rbkmoney.machinarium.domain.TMachineEvent;

public interface MachineEventHandler {
    SignalResultData<ScheduleChange> handleEvent(TMachine<ScheduleChange> machine,
                                                 TMachineEvent<ScheduleChange> event,
                                                 DefaultMachineEventChain filterChain);
}
