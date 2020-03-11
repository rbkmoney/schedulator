package com.rbkmoney.schedulator.service;

import com.rbkmoney.damsel.schedule.ScheduleChange;
import com.rbkmoney.damsel.schedule.ScheduleJobRegistered;
import com.rbkmoney.damsel.schedule.ScheduledJobContext;
import com.rbkmoney.machinarium.domain.TMachine;
import com.rbkmoney.schedulator.service.model.ScheduleJobCalculateResult;

import java.time.Instant;

public interface ScheduleJobService {

    ScheduleJobCalculateResult calculateNextExecutionTime(TMachine<ScheduleChange> machine, ScheduleJobRegistered scheduleJobRegistered);

    ScheduledJobContext calculateScheduledJobContext(ScheduleJobRegistered scheduleJobRegistered);

    ScheduledJobContext calculateRetryJobContext(ScheduleJobRegistered scheduleJobRegistered, Instant machineTimer);

}
