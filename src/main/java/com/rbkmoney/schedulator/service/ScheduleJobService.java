package com.rbkmoney.schedulator.service;

import com.rbkmoney.damsel.schedule.ScheduleJobRegistered;
import com.rbkmoney.damsel.schedule.ScheduledJobContext;

import java.time.Instant;

public interface ScheduleJobService {

    ScheduledJobContext calculateScheduledJobContext(ScheduleJobRegistered scheduleJobRegistered);

    ScheduledJobContext calculateRetryJobContext(ScheduleJobRegistered scheduleJobRegistered, Instant machineTimer);

}
