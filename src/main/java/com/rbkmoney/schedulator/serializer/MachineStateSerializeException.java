package com.rbkmoney.schedulator.serializer;

public class MachineStateSerializeException extends RuntimeException {
    public MachineStateSerializeException() {
        super();
    }

    public MachineStateSerializeException(String message) {
        super(message);
    }

    public MachineStateSerializeException(String message, Throwable cause) {
        super(message, cause);
    }

    public MachineStateSerializeException(Throwable cause) {
        super(cause);
    }
}
