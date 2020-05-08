package com.rbkmoney.schedulator.service;

import com.rbkmoney.damsel.schedule.ScheduleJobRegistered;
import com.rbkmoney.damsel.schedule.ScheduledJobContext;
import com.rbkmoney.schedulator.serializer.MachineTimerState;
import com.rbkmoney.schedulator.service.model.ScheduleJobCalculateResult;

public interface ScheduleJobService {

    ScheduleJobCalculateResult calculateNextExecutionTime(ScheduleJobRegistered scheduleJobRegistered,
                                                          MachineTimerState machineTimer);

    ScheduledJobContext calculateScheduledJobContext(ScheduleJobRegistered scheduleJobRegistered);

}
