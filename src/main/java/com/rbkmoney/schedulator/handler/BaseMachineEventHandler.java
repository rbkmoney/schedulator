package com.rbkmoney.schedulator.handler;

import com.rbkmoney.machinarium.domain.SignalResultData;
import com.rbkmoney.machinarium.domain.TMachine;
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
    public final SignalResultData<T> handle(TMachine<T> machine, TMachineEvent<T> machineEvent) throws MachineEventHandleException {
        if (canHandle(machineEvent)) {
            return handleEvent(machine, machineEvent);
        }
        if (nextMachineEventHandler != null) {
            return nextMachineEventHandler.handle(machine, machineEvent);
        } else {
            throw new MachineEventHandleException(String.format("Empty next event handler. Event '%s'", machineEvent));
        }
    }

    protected abstract SignalResultData<T> handleEvent(TMachine<T> machine, TMachineEvent<T> machineEvent) throws MachineEventHandleException;

    protected abstract boolean canHandle(TMachineEvent<T> machineEvent);

}
