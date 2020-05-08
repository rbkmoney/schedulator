package com.rbkmoney.schedulator.backoff;

public interface JobBackOff {

    long nextBackOff();

}
