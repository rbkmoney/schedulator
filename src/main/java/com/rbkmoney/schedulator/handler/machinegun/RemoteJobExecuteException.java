package com.rbkmoney.schedulator.handler.machinegun;

import lombok.Getter;

@Getter
public class RemoteJobExecuteException extends RuntimeException {

    private final String url;

    public RemoteJobExecuteException(String url) {
        this.url = url;
    }

    public RemoteJobExecuteException(String url, Throwable cause) {
        super(cause);
        this.url = url;
    }

}
