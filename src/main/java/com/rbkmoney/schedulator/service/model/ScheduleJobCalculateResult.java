package com.rbkmoney.schedulator.service.model;

import com.rbkmoney.damsel.schedule.ExecuteJobRequest;
import com.rbkmoney.damsel.schedule.ScheduledJobContext;
import com.rbkmoney.schedulator.serializer.MachineTimerState;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@Builder
public class ScheduleJobCalculateResult {
    private final ExecuteJobRequest executeJobRequest;

    private final ScheduledJobContext scheduledJobContext;

    private final ByteBuffer remoteJobContext;

    private final MachineTimerState machineTimerState;
}
