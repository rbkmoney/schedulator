package com.rbkmoney.schedulator.service;

import com.rbkmoney.damsel.schedule.ScheduleJobRegistered;
import com.rbkmoney.damsel.schedule.ScheduledJobContext;

public interface ScheduleJobService {

    ScheduledJobContext getScheduledJobContext(ScheduleJobRegistered scheduleJobRegistered);

}
