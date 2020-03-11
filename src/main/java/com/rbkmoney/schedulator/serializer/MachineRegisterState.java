package com.rbkmoney.schedulator.serializer;

import lombok.Data;

@Data
public class MachineRegisterState {

    private RegisterContext context;

    private String executorServicePath;

    private String schedulerId;

    private Long dominantRevisionId;

    private Integer businessSchedulerId;

    private Integer calendarId;

}
