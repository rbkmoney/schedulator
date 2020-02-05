package com.rbkmoney.schedulator.service.impl;

public class ScheduleJobCalculateException extends RuntimeException {
    public ScheduleJobCalculateException() {
        super();
    }

    public ScheduleJobCalculateException(String message) {
        super(message);
    }

    public ScheduleJobCalculateException(String message, Throwable cause) {
        super(message, cause);
    }

    public ScheduleJobCalculateException(Throwable cause) {
        super(cause);
    }
}
