package com.rbkmoney.schedulator.serializer;

public interface MachineStateSerializer {

    byte[] serialize(SchedulatorMachineState state);

    SchedulatorMachineState deserializer(byte[] state);

}
