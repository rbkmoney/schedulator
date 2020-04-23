package com.rbkmoney.schedulator.service;

import com.rbkmoney.damsel.schedule.ScheduleChange;
import com.rbkmoney.damsel.schedule.ScheduleJobRegistered;
import com.rbkmoney.damsel.schedule.ScheduledJobContext;
import com.rbkmoney.machinarium.domain.TMachine;
import com.rbkmoney.schedulator.serializer.MachineTimerState;
import com.rbkmoney.schedulator.service.model.ScheduleJobCalculateResult;

public interface ScheduleJobService {

    ScheduleJobCalculateResult calculateNextExecutionTime(TMachine<ScheduleChange> machine,
                                                          ScheduleJobRegistered scheduleJobRegistered,
                                                          MachineTimerState machineTimerState);

    ScheduledJobContext calculateScheduledJobContext(ScheduleJobRegistered scheduleJobRegistered);

    ScheduledJobContext calculateRetryJobContext(ScheduleJobRegistered scheduleJobRegistered, MachineTimerState machineTimer);

}
