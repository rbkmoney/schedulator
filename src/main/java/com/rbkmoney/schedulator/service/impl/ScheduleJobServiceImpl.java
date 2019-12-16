package com.rbkmoney.schedulator.service.impl;

import com.rbkmoney.damsel.domain.BusinessSchedule;
import com.rbkmoney.damsel.domain.Calendar;
import com.rbkmoney.damsel.schedule.DominantBasedSchedule;
import com.rbkmoney.damsel.schedule.ScheduleJobRegistered;
import com.rbkmoney.damsel.schedule.ScheduledJobContext;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.schedulator.cron.SchedulerCalculator;
import com.rbkmoney.schedulator.cron.SchedulerComputeResult;
import com.rbkmoney.schedulator.service.DominantService;
import com.rbkmoney.schedulator.service.ScheduleJobService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.ZonedDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class ScheduleJobServiceImpl implements ScheduleJobService {

    private final DominantService dominantService;

    @Override
    public ScheduledJobContext getScheduledJobContext(ScheduleJobRegistered scheduleJobRegistered) {
        DominantBasedSchedule dominantSchedule = scheduleJobRegistered.getSchedule().getDominantSchedule();
        log.info("Get scheduler job context from dominant: {}", dominantSchedule);
        BusinessSchedule businessSchedule = dominantService.getBusinessSchedule(dominantSchedule.getBusinessScheduleRef(), dominantSchedule.getRevision());
        Calendar calendar = dominantService.getCalendar(dominantSchedule.getCalendarRef(), dominantSchedule.getRevision());

        return buildScheduleJobContext(calendar, businessSchedule);
    }

    private ScheduledJobContext buildScheduleJobContext(Calendar calendar, BusinessSchedule schedule) {
        SchedulerCalculator schedulerCalculator = SchedulerCalculator.newSchedulerCalculator(ZonedDateTime.now(), calendar, schedule);
        SchedulerComputeResult calcResult = schedulerCalculator.computeFireTime();

        String prevFireTime = TypeUtil.temporalToString(calcResult.getPrevFireTime());
        String nextFireTime = TypeUtil.temporalToString(calcResult.getNextFireTime());
        String cronFireTime = TypeUtil.temporalToString(calcResult.getNextCronFireTime());

        return new ScheduledJobContext(nextFireTime, prevFireTime, cronFireTime);
    }

}
