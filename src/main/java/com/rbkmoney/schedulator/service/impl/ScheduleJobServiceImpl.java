package com.rbkmoney.schedulator.service.impl;

import com.rbkmoney.damsel.domain.BusinessSchedule;
import com.rbkmoney.damsel.domain.Calendar;
import com.rbkmoney.damsel.schedule.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.schedulator.backoff.JobExponentialBackOff;
import com.rbkmoney.schedulator.config.properties.JobRetryConfig;
import com.rbkmoney.schedulator.cron.SchedulerCalculator;
import com.rbkmoney.schedulator.cron.SchedulerComputeResult;
import com.rbkmoney.schedulator.exception.NotFoundException;
import com.rbkmoney.schedulator.handler.machinegun.RemoteJobExecuteException;
import com.rbkmoney.schedulator.serializer.MachineStateSerializer;
import com.rbkmoney.schedulator.serializer.MachineTimerState;
import com.rbkmoney.schedulator.service.DominantService;
import com.rbkmoney.schedulator.service.RemoteClientManager;
import com.rbkmoney.schedulator.service.ScheduleJobService;
import com.rbkmoney.schedulator.service.model.ScheduleJobCalculateResult;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

    private final MachineStateSerializer machineStateSerializer;

    private final RemoteClientManager remoteClientManager;

    private final DominantService dominantService;

    private final JobRetryConfig jobRetryConfig;

    public ScheduleJobCalculateResult calculateNextExecutionTime(ScheduleJobRegistered scheduleJobRegistered,
                                                                 MachineTimerState machineTimerState) {
        // Calculate execution time
        log.trace("Calculate execution time");
        ExecuteJobRequest executeJobRequest = new ExecuteJobRequest();
        ScheduledJobContext scheduledJobContext = calculateScheduledJobContext(scheduleJobRegistered);
        executeJobRequest.setScheduledJobContext(scheduledJobContext);
        executeJobRequest.setServiceExecutionContext(scheduleJobRegistered.getContext());

        String url = scheduleJobRegistered.getExecutorServicePath();
        MachineTimerState resultMachineTimerState = new MachineTimerState();
        resultMachineTimerState.setJobRetryCount(machineTimerState.getJobRetryCount());
        resultMachineTimerState.setCurrentInterval(machineTimerState.getCurrentInterval());
        ByteBuffer remoteJobContext = null;
        try {
            remoteJobContext = callRemoteJob(url, executeJobRequest);
            resultMachineTimerState.setJobRetryCount(0); // Reset retry count
        } catch (RemoteJobExecuteException e) {
            // Calculate retry execution time
            resultMachineTimerState.setJobRetryCount(resultMachineTimerState.getJobRetryCount() + 1);
            if (machineTimerState.getJobRetryCount() > jobRetryConfig.getJob().getMaxAttempts()) {
                throw new ScheduleJobCalculateException("Max retry count exceeded");
            }
            log.trace("Calculate retry execution time");
            remoteJobContext = ByteBuffer.wrap(scheduleJobRegistered.getContext()); // Set old execution context

            RetryJobContext retryJobContext = calculateRetryJobContext(scheduleJobRegistered, machineTimerState);
            scheduledJobContext = retryJobContext.getScheduledJobContext(); // retry context
            resultMachineTimerState.setCurrentInterval(retryJobContext.getCurrentInterval());
        }

        return ScheduleJobCalculateResult.builder()
                .executeJobRequest(executeJobRequest)
                .scheduledJobContext(scheduledJobContext)
                .remoteJobContext(remoteJobContext)
                .machineTimerState(resultMachineTimerState)
                .build();
    }

    @Override
    public ScheduledJobContext calculateScheduledJobContext(ScheduleJobRegistered scheduleJobRegistered) {
        DominantBasedSchedule dominantSchedule = scheduleJobRegistered.getSchedule().getDominantSchedule();
        try {
            log.debug("Get scheduler job context from dominant: {}", dominantSchedule);
            BusinessSchedule businessSchedule = dominantService.getBusinessSchedule(
                    dominantSchedule.getBusinessScheduleRef(), dominantSchedule.getRevision());
            Calendar calendar = dominantService.getCalendar(dominantSchedule.getCalendarRef(), dominantSchedule.getRevision());

            TimeZone timeZone = TimeZone.getTimeZone(calendar.getTimezone());
            SchedulerCalculator schedulerCalculator = SchedulerCalculator.newSchedulerCalculator(
                    ZonedDateTime.now(timeZone.toZoneId()), calendar, businessSchedule);
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

    private RetryJobContext calculateRetryJobContext(ScheduleJobRegistered scheduleJobRegistered,
                                                     MachineTimerState timerState) {
        DominantBasedSchedule dominantSchedule = scheduleJobRegistered.getSchedule().getDominantSchedule();
        try {
            Calendar calendar = dominantService.getCalendar(dominantSchedule.getCalendarRef(), dominantSchedule.getRevision());
            TimeZone timeZone = TimeZone.getTimeZone(calendar.getTimezone());

            JobExponentialBackOff backOff = new JobExponentialBackOff(
                    jobRetryConfig.getJob().getMaxIntervalSeconds(),
                    jobRetryConfig.getJob().getInitialIntervalSeconds(),
                    timerState.getCurrentInterval());
            Instant now = Instant.now(Clock.system(timeZone.toZoneId()));
            long interval = backOff.nextBackOff();

            String nextFireTime = TypeUtil.temporalToString(now.plusSeconds(interval));

            ScheduledJobContext scheduledJobContext = new ScheduledJobContext(nextFireTime, null, null);

            return new RetryJobContext(scheduledJobContext, interval);
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
            throw new RemoteJobExecuteException(url, e);
        }
    }

    @Data
    private static final class RetryJobContext {

        private final ScheduledJobContext scheduledJobContext;

        private final long currentInterval;

    }

}
