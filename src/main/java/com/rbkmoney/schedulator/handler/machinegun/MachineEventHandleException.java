package com.rbkmoney.schedulator.handler.machinegun;

public class MachineEventHandleException extends RuntimeException {

    public MachineEventHandleException() {
        super();
    }

    public MachineEventHandleException(String message) {
        super(message);
    }

    public MachineEventHandleException(String message, Throwable cause) {
        super(message, cause);
    }

    public MachineEventHandleException(Throwable cause) {
        super(cause);
    }
}
