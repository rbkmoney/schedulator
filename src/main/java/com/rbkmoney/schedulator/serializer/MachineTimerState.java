package com.rbkmoney.schedulator.serializer;

import lombok.Data;

import java.time.Instant;

@Data
public class MachineTimerState {

    private Instant nextTimer;

}
