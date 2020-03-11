package com.rbkmoney.schedulator.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Base64;

public class RegisterContextSerializer extends StdSerializer<RegisterContext> {

    public RegisterContextSerializer() {
        super(RegisterContext.class);
    }

    @Override
    public void serialize(RegisterContext value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeString(Base64.getEncoder().encodeToString(value.getBytes()));
    }
}
