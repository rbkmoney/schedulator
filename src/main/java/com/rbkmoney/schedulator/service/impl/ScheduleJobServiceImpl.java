package com.rbkmoney.schedulator.service.impl;

import com.rbkmoney.damsel.domain.BusinessSchedule;
import com.rbkmoney.damsel.domain.Calendar;
import com.rbkmoney.damsel.schedule.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinarium.domain.TMachine;
import com.rbkmoney.schedulator.cron.SchedulerCalculator;
import com.rbkmoney.schedulator.cron.SchedulerComputeResult;
import com.rbkmoney.schedulator.exception.NotFoundException;
import com.rbkmoney.schedulator.service.DominantService;
import com.rbkmoney.schedulator.service.RemoteClientManager;
import com.rbkmoney.schedulator.service.ScheduleJobService;
import com.rbkmoney.schedulator.service.model.ScheduleJobCalculateResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.TimeZone;

@Slf4j
@Service
@RequiredArgsConstructor
public class ScheduleJobServiceImpl implements ScheduleJobService {

    private final RemoteClientManager remoteClientManager;

    private final DominantService dominantService;

    @Value("${retry-policy.job.intervalSeconds:30}")
    private int retryInterval;

    public ScheduleJobCalculateResult calculateNextExecutionTime(TMachine<ScheduleChange> machine,
                                                                 ScheduleJobRegistered scheduleJobRegistered) {
        // Calculate execution time
        ExecuteJobRequest executeJobRequest = new ExecuteJobRequest();
        ScheduledJobContext scheduledJobContext = calculateScheduledJobContext(scheduleJobRegistered);
        executeJobRequest.setScheduledJobContext(scheduledJobContext);
        executeJobRequest.setServiceExecutionContext(scheduleJobRegistered.getContext());

        String url = scheduleJobRegistered.getExecutorServicePath();
        ByteBuffer remoteJobContext = callRemoteJob(url, executeJobRequest);

        // Calculate retry execution time
        if (remoteJobContext == null) {
            remoteJobContext = ByteBuffer.wrap(scheduleJobRegistered.getContext()); // Set old execution context
            scheduledJobContext = calculateRetryJobContext(scheduleJobRegistered, machine.getTimer());
        }

        return new ScheduleJobCalculateResult(executeJobRequest, scheduledJobContext, remoteJobContext);
    }

    @Override
    public ScheduledJobContext calculateScheduledJobContext(ScheduleJobRegistered scheduleJobRegistered) {
        DominantBasedSchedule dominantSchedule = scheduleJobRegistered.getSchedule().getDominantSchedule();
        try {
            log.info("Get scheduler job context from dominant: {}", dominantSchedule);
            BusinessSchedule businessSchedule = dominantService.getBusinessSchedule(dominantSchedule.getBusinessScheduleRef(), dominantSchedule.getRevision());
            Calendar calendar = dominantService.getCalendar(dominantSchedule.getCalendarRef(), dominantSchedule.getRevision());

            SchedulerCalculator schedulerCalculator = SchedulerCalculator.newSchedulerCalculator(ZonedDateTime.now(), calendar, businessSchedule);
            SchedulerComputeResult calcResult = schedulerCalculator.computeFireTime();

            String prevFireTime = TypeUtil.temporalToString(calcResult.getPrevFireTime());
            String nextFireTime = TypeUtil.temporalToString(calcResult.getNextFireTime());
            String cronFireTime = TypeUtil.temporalToString(calcResult.getNextCronFireTime());

            return new ScheduledJobContext(nextFireTime, prevFireTime, cronFireTime);
        } catch (NotFoundException e) {
            throw new ScheduleJobCalculateException(
                    String.format("Can't find 'businessSchedule' from dominant: %s", dominantSchedule), e);
        } catch (Exception e) {
            throw new ScheduleJobCalculateException(
                    String.format("Exception while calculate schedule for: %s", scheduleJobRegistered), e);
        }
    }

    @Override
    public ScheduledJobContext calculateRetryJobContext(ScheduleJobRegistered scheduleJobRegistered, Instant machineTimer) {
        DominantBasedSchedule dominantSchedule = scheduleJobRegistered.getSchedule().getDominantSchedule();
        try {
            Calendar calendar = dominantService.getCalendar(dominantSchedule.getCalendarRef(), dominantSchedule.getRevision());
            TimeZone timeZone = TimeZone.getTimeZone(calendar.getTimezone());

            if (machineTimer == null) {
                machineTimer = Instant.now(Clock.system(timeZone.toZoneId())); // Set timer from now
            }

            String nextFireTime = TypeUtil.temporalToString(machineTimer.plusSeconds(retryInterval));

            return new ScheduledJobContext(nextFireTime, null, null);
        } catch (NotFoundException e) {
            throw new ScheduleJobCalculateException(
                    String.format("Can't find 'businessSchedule' from dominant: %s", dominantSchedule), e);
        } catch (Exception e) {
            throw new ScheduleJobCalculateException(
                    String.format("Exception while calculate schedule for: %s", scheduleJobRegistered), e);
        }
    }

    private ByteBuffer callRemoteJob(String url, ExecuteJobRequest executeJobRequest) {
        try {
            ScheduledJobExecutorSrv.Iface remoteClient = remoteClientManager.getRemoteClient(url);
            return remoteClient.executeJob(executeJobRequest);
        } catch (Exception e) {
            log.error("Call '{}' job failed. Set old remoteJobContext variable", url, e);
            return null;
        }
    }

}
