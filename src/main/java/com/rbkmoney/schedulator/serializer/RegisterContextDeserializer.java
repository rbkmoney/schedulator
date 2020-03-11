package com.rbkmoney.schedulator.serializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.Base64;

public class RegisterContextDeserializer extends StdDeserializer<RegisterContext> {

    protected RegisterContextDeserializer() {
        super(RegisterContext.class);
    }

    @Override
    public RegisterContext deserialize(JsonParser p, DeserializationContext ctx) throws IOException, JsonProcessingException {
        JsonNode node = p.getCodec().readTree(p);
        String base64 = node.asText();
        return new RegisterContext(Base64.getDecoder().decode(base64));
    }
}
