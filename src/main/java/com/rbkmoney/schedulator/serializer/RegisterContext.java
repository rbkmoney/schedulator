package com.rbkmoney.schedulator.serializer;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Data;

@Data
@JsonSerialize(using = RegisterContextSerializer.class)
@JsonDeserialize(using = RegisterContextDeserializer.class)
public class RegisterContext {
    private final byte[] bytes;
}
