package com.rbkmoney.schedulator.handler;

import com.rbkmoney.machinarium.domain.SignalResultData;
import com.rbkmoney.machinarium.domain.TMachineEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseMachineEventHandler<T> implements MachineEventHandler<T> {

    protected final Logger log;

    protected final MachineEventHandler<T> nextMachineEventHandler;

    protected BaseMachineEventHandler(MachineEventHandler<T> nextMachineEventHandler) {
        this.nextMachineEventHandler = nextMachineEventHandler;
        this.log = LoggerFactory.getLogger(getClass());
    }

    @Override
    public final SignalResultData<T> handle(TMachineEvent<T> machineEvent) throws MachineEventHandleException {
        if (isNeedToHandle(machineEvent)) {
            return handleEvent(machineEvent);
        } else {
            if (nextMachineEventHandler != null) {
                 return nextMachineEventHandler.handle(machineEvent);
            }
        }
        return null;
    }

    protected abstract SignalResultData<T> handleEvent(TMachineEvent<T> machineEvent) throws MachineEventHandleException;

    protected abstract boolean isNeedToHandle(TMachineEvent<T> machineEvent);

}
