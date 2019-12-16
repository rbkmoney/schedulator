package com.rbkmoney.schedulator.handler;

public class MachineEventHandleException extends Exception {

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
