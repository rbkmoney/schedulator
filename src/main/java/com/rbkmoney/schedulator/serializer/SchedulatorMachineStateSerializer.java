package com.rbkmoney.schedulator.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@RequiredArgsConstructor
public class SchedulatorMachineStateSerializer implements MachineStateSerializer {

    private final ObjectMapper objectMapper;

    @Override
    public byte[] serialize(SchedulatorMachineState state) {
        try {
            return objectMapper.writeValueAsBytes(state);
        } catch (JsonProcessingException e) {
            throw new MachineStateSerializeException(e);
        }
    }

    @Override
    public SchedulatorMachineState deserializer(byte[] state) {
        try {
            return objectMapper.readValue(state, SchedulatorMachineState.class);
        } catch (IOException e) {
            throw new MachineStateSerializeException(e);
        }
    }
}
