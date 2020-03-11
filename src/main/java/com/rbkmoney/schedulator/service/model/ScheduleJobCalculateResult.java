package com.rbkmoney.schedulator.service.model;

import com.rbkmoney.damsel.schedule.ExecuteJobRequest;
import com.rbkmoney.damsel.schedule.ScheduledJobContext;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
public class ScheduleJobCalculateResult {
    private final ExecuteJobRequest executeJobRequest;

    private final ScheduledJobContext scheduledJobContext;

    private final ByteBuffer remoteJobContext;
}
