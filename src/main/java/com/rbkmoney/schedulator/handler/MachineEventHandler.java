package com.rbkmoney.schedulator.handler;

import com.rbkmoney.machinarium.domain.SignalResultData;
import com.rbkmoney.machinarium.domain.TMachineEvent;

public interface MachineEventHandler<T> {

    SignalResultData<T> handle(TMachineEvent<T> machineEvent) throws MachineEventHandleException;

}
