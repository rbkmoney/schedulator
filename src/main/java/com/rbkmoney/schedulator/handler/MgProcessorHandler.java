package com.rbkmoney.schedulator.handler;

import com.rbkmoney.damsel.domain.BusinessSchedule;
import com.rbkmoney.damsel.domain.Calendar;
import com.rbkmoney.damsel.schedule.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinarium.domain.CallResultData;
import com.rbkmoney.machinarium.domain.SignalResultData;
import com.rbkmoney.machinarium.domain.TMachineEvent;
import com.rbkmoney.machinarium.handler.AbstractProcessorHandler;
import com.rbkmoney.machinegun.base.Timer;
import com.rbkmoney.machinegun.msgpack.Nil;
import com.rbkmoney.machinegun.msgpack.Value;
import com.rbkmoney.machinegun.stateproc.*;
import com.rbkmoney.schedulator.cron.SchedulerCalculator;
import com.rbkmoney.schedulator.cron.SchedulerCalculatorConfig;
import com.rbkmoney.schedulator.cron.SchedulerComputeResult;
import com.rbkmoney.schedulator.exception.NotFoundException;
import com.rbkmoney.schedulator.service.DominantService;
import com.rbkmoney.schedulator.util.SchedulerUtil;
import com.rbkmoney.woody.api.flow.error.WUnavailableResultException;
import com.rbkmoney.woody.api.flow.error.WUndefinedResultException;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
@Component
public class MgProcessorHandler extends AbstractProcessorHandler<ScheduleChange, ScheduleChange> {

    private final DominantService dominantService;

    private final RemoteClientManager remoteClientManager;

    public MgProcessorHandler(DominantService dominantService, RemoteClientManager remoteClientManager) {
        super(ScheduleChange.class, ScheduleChange.class);
        this.dominantService = dominantService;
        this.remoteClientManager = remoteClientManager;
    }

    @Override
    protected SignalResultData<ScheduleChange> processSignalInit(String namespace,
                                                                 String machineId,
                                                                 Content machineState,
                                                                 ScheduleChange scheduleChangeRegistered) {
        log.info("Request processSignalInit() machineId: {} scheduleChangeRegistered: {}", machineId, scheduleChangeRegistered);
        ScheduleJobRegistered scheduleJobRegistered = scheduleChangeRegistered.getScheduleJobRegistered();
        try {
            ByteBuffer contextValidationRequest = ByteBuffer.wrap(scheduleJobRegistered.getContext());
            ContextValidationResponse contextValidationResponse = validateExecutionContext(scheduleJobRegistered.getExecutorServicePath(), contextValidationRequest);

            log.info("Context validation response: {}", contextValidationResponse);

            ScheduleContextValidated scheduleContextValidated = new ScheduleContextValidated(contextValidationRequest, contextValidationResponse);
            ScheduleChange scheduleChangeValidated = ScheduleChange.schedule_context_validated(scheduleContextValidated);
            ScheduledJobContext scheduledJobContext = getScheduledJobContext(scheduleJobRegistered);
            ComplexAction complexAction = buildComplexAction(scheduledJobContext.getNextFireTime());
            SignalResultData<ScheduleChange> signalResultData = new SignalResultData<>(
                    Value.nl(new Nil()),
                    Arrays.asList(scheduleChangeRegistered, scheduleChangeValidated),
                    complexAction);
            log.info("Response of processSignalInit: {}", signalResultData);

            return signalResultData;
        } catch (WUnavailableResultException e) {
            log.warn("Couldn't call remote service. We will try again.", e);
            throw e;
        } catch (Exception e) {
            log.warn("Couldn't processSignalInit, machineId={}, scheduleChangeRegistered={}", machineId, scheduleChangeRegistered, e);
            throw new WUndefinedResultException(e);
        }
    }

    @Override
    protected SignalResultData<ScheduleChange> processSignalTimeout(String namespace,
                                                                    String machineId,
                                                                    Content machineState,
                                                                    List<TMachineEvent<ScheduleChange>> list) {
        log.info("Request processSignalTimeout() machineId: {} list: {}", machineId, list);
        try {
            ScheduleJobRegistered scheduleJobRegistered = list.stream()
                    .filter(e -> e.getData().isSetScheduleJobRegistered())
                    .findFirst()
                    .orElseThrow(() -> new NotFoundException("Couldn't found ScheduleJobRegistered for machineId = " + machineId))
                    .getData().getScheduleJobRegistered();
            String url = scheduleJobRegistered.getExecutorServicePath();

            ExecuteJobRequest executeJobRequest = new ExecuteJobRequest();
            ScheduledJobContext scheduledJobContext = getScheduledJobContext(scheduleJobRegistered);
            executeJobRequest.setScheduledJobContext(scheduledJobContext);
            executeJobRequest.setServiceExecutionContext(scheduleJobRegistered.getContext());

            // Execute remote client
            log.info("Execute job for '{}'", url);
            ScheduledJobExecutorSrv.Iface remoteClient = remoteClientManager.getRemoteClient(url);
            ByteBuffer genericServiceExecutionContext = remoteClient.executeJob(executeJobRequest);

            ScheduleChange scheduleChange = ScheduleChange.schedule_job_executed(new ScheduleJobExecuted(executeJobRequest, genericServiceExecutionContext));
            ComplexAction complexAction = buildComplexAction(scheduledJobContext.getNextFireTime());

            SignalResultData<ScheduleChange> signalResultData = new SignalResultData<>(
                    Value.nl(new Nil()),
                    Collections.singletonList(scheduleChange),
                    complexAction);
            log.info("Response of processSignalTimeout: {}", signalResultData);

            return signalResultData;
        } catch (WUnavailableResultException e) {
            log.warn("Couldn't call remote service. We will try again.", e);
            throw e;
        } catch (Exception e) {
            log.warn("Couldn't processSignalTimeout, machineId={}", machineId, e);
            throw new WUndefinedResultException(e);
        }
    }

    @Override
    protected CallResultData<ScheduleChange> processCall(String namespace,
                                                         String machineId,
                                                         ScheduleChange scheduleChange,
                                                         List<TMachineEvent<ScheduleChange>> machineEvents) {
        log.info("Request processCall() machineId: {} scheduleChange: {} machineEvents: {}", machineId, scheduleChange, machineEvents);
        ComplexAction complexAction = new ComplexAction();
        TimerAction timer = new TimerAction();
        timer.setUnsetTimer(new UnsetTimerAction());
        complexAction.setTimer(timer);
        CallResultData<ScheduleChange> callResultData = new CallResultData<>(
                Value.nl(new Nil()),
                scheduleChange,
                Collections.singletonList(scheduleChange),
                complexAction);
        log.info("Response of processCall: {}", callResultData);
        return callResultData;
    }

    private ContextValidationResponse validateExecutionContext(String url, ByteBuffer context) throws TException {
        log.info("Call validation context for '{}'", url);
        ScheduledJobExecutorSrv.Iface client = remoteClientManager.getRemoteClient(url);
        try {
            return client.validateExecutionContext(context);
        } catch (Exception e) {
            throw new WUnavailableResultException(e);
        }
    }

    private SchedulerCalculator buildSchedulerCalculator(Calendar calendar, BusinessSchedule schedule) {
        List<String> cronList = SchedulerUtil.buildCron(schedule.getSchedule(), Optional.ofNullable(calendar.getFirstDayOfWeek()));
        ZonedDateTime currentDateTime = ZonedDateTime.now();
        String cron = SchedulerUtil.getNearestCron(cronList, currentDateTime);
        SchedulerCalculator schedulerCalculator = null;
        if (schedule.isSetDelay()) {
            SchedulerCalculatorConfig calculatorConfig = SchedulerCalculatorConfig.builder()
                    .startTime(currentDateTime.toLocalDateTime())
                    .months(schedule.getDelay().getMonths())
                    .days(schedule.getDelay().getDays())
                    .hours(schedule.getDelay().getHours())
                    .minutes(schedule.getDelay().getMinutes())
                    .seconds(schedule.getDelay().getSeconds())
                    .build();
            schedulerCalculator = new SchedulerCalculator(cron, calendar, calculatorConfig);
        } else {
            SchedulerCalculatorConfig calculatorConfig = SchedulerCalculatorConfig.builder()
                    .startTime(currentDateTime.toLocalDateTime())
                    .build();
            schedulerCalculator = new SchedulerCalculator(cron, calendar, calculatorConfig);
        }
        return schedulerCalculator;
    }

    private ComplexAction buildComplexAction(String deadline) {
        ComplexAction complexAction = new ComplexAction();
        TimerAction timer = new TimerAction();
        SetTimerAction setTimerAction = new SetTimerAction();
        setTimerAction.setTimer(Timer.deadline(deadline));
        timer.setSetTimer(setTimerAction);
        complexAction.setTimer(timer);
        return complexAction;
    }

    private ScheduledJobContext getScheduledJobContext(ScheduleJobRegistered scheduleJobRegistered) {
        DominantBasedSchedule dominantSchedule = scheduleJobRegistered.getSchedule().getDominantSchedule();
        log.info("Get scheduler job context from dominant: {}", dominantSchedule);
        BusinessSchedule businessSchedule = dominantService.getBusinessSchedule(dominantSchedule.getBusinessScheduleRef(), dominantSchedule.getRevision());
        Calendar calendar = dominantService.getCalendar(dominantSchedule.getCalendarRef(), dominantSchedule.getRevision());
        return buildScheduleJobContext(calendar, businessSchedule);
    }

    private ScheduledJobContext buildScheduleJobContext(Calendar calendar, BusinessSchedule schedule) {
        SchedulerCalculator schedulerCalculator = buildSchedulerCalculator(calendar, schedule);
        SchedulerComputeResult calcResult = schedulerCalculator.computeFireTime();

        String prevFireTime = TypeUtil.temporalToString(calcResult.getPrevFireTime());
        String nextFireTime = TypeUtil.temporalToString(calcResult.getNextFireTime());
        String cronFireTime = TypeUtil.temporalToString(calcResult.getNextCronFireTime());

        return new ScheduledJobContext(nextFireTime, prevFireTime, cronFireTime);
    }

}
