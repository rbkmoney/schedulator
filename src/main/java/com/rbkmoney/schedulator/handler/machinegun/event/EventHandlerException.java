package com.rbkmoney.schedulator.handler.machinegun.event;

public class EventHandlerException extends RuntimeException {
    public EventHandlerException() {
        super();
    }

    public EventHandlerException(String message) {
        super(message);
    }

    public EventHandlerException(String message, Throwable cause) {
        super(message, cause);
    }

    public EventHandlerException(Throwable cause) {
        super(cause);
    }
}
