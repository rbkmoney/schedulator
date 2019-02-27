package com.rbkmoney.schedulator.handler;

import com.rbkmoney.damsel.base.TimeSpan;
import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.domain.Calendar;
import com.rbkmoney.damsel.schedule.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinarium.domain.CallResultData;
import com.rbkmoney.machinarium.domain.SignalResultData;
import com.rbkmoney.machinarium.domain.TMachineEvent;
import com.rbkmoney.machinarium.handler.AbstractProcessorHandler;
import com.rbkmoney.machinegun.base.Timer;
import com.rbkmoney.machinegun.stateproc.ComplexAction;
import com.rbkmoney.machinegun.stateproc.SetTimerAction;
import com.rbkmoney.machinegun.stateproc.TimerAction;
import com.rbkmoney.machinegun.stateproc.UnsetTimerAction;
import com.rbkmoney.schedulator.exception.NotFoundException;
import com.rbkmoney.schedulator.service.DominantService;
import com.rbkmoney.schedulator.trigger.FreezeTimeCronScheduleBuilder;
import com.rbkmoney.schedulator.trigger.FreezeTimeCronTrigger;
import com.rbkmoney.schedulator.util.SchedulerUtil;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.quartz.TriggerBuilder;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
public class MgProcessorHandler extends AbstractProcessorHandler<ScheduleChange, ScheduleChange> {

    private final DominantService dominantService;

    public MgProcessorHandler(DominantService dominantService) {
        super(ScheduleChange.class, ScheduleChange.class);
        this.dominantService = dominantService;
    }

    private ContextValidationResponse validateExecutionContext(String url, ByteBuffer context) throws TException {
        ScheduledJobExecutorSrv.Iface client = getRemoteClient(url);
        return client.validateExecutionContext(context);
    }

    private ScheduledJobExecutorSrv.Iface getRemoteClient(String url) {
        return new THSpawnClientBuilder()
                    .withAddress(URI.create(url))
                    .build(ScheduledJobExecutorSrv.Iface.class);
    }

    private ScheduledJobContext getScheduledJobContext(Calendar calendar, BusinessSchedule schedule) {
        List<String> cronList = SchedulerUtil.buildCron(schedule.getSchedule(), Optional.ofNullable(calendar.getFirstDayOfWeek()));
        List<FreezeTimeCronTrigger> freezeTimeCronTriggers = cronList.stream().map(cron -> {
            FreezeTimeCronScheduleBuilder freezeTimeCronScheduleBuilder = FreezeTimeCronScheduleBuilder.cronSchedule(cron)
                    .inTimeZone(TimeZone.getTimeZone(calendar.getTimezone()));
            if (schedule.isSetDelay() || schedule.isSetPolicy()) {
                TimeSpan timeSpan = Optional.ofNullable(schedule.getPolicy())
                        .map(PayoutCompilationPolicy::getAssetsFreezeFor)
                        .orElse(schedule.getDelay());
                freezeTimeCronScheduleBuilder.withYears(timeSpan.getYears())
                        .withMonths(timeSpan.getMonths())
                        .withDays(timeSpan.getDays())
                        .withHours(timeSpan.getHours())
                        .withMinutes(timeSpan.getMinutes())
                        .withSeconds(timeSpan.getSeconds());
            }
            return TriggerBuilder.newTrigger().withSchedule(freezeTimeCronScheduleBuilder).build();
        }).collect(Collectors.toList());

        String nextFireTime = freezeTimeCronTriggers.stream()
                .map(t -> t.getNextFireTime().toInstant())
                .min(Comparator.naturalOrder())
                .map(TypeUtil::temporalToString)
                .orElseThrow(() -> new RuntimeException("Couldn't compute next fire time"));

        String prevFireTime = freezeTimeCronTriggers.stream()
                .map(t -> t.getPreviousFireTime().toInstant())
                .min(Comparator.naturalOrder())
                .map(TypeUtil::temporalToString)
                .orElseThrow(() -> new RuntimeException("Couldn't compute prev fire time"));


        String nextCronTime = freezeTimeCronTriggers.stream()
                .map(t -> t.getNextCronTime().toInstant())
                .min(Comparator.naturalOrder())
                .map(TypeUtil::temporalToString)
                .orElseThrow(() -> new RuntimeException("Couldn't compute next cron time"));

        return new ScheduledJobContext(nextFireTime, prevFireTime, nextCronTime);

    }

    @Override
    protected SignalResultData<ScheduleChange> processSignalInit(String namespace, String machineId, ScheduleChange scheduleChangeRegistered) {
        log.info("Request processSignalInit() machineId: {} scheduleChangeRegistered: {}", machineId, scheduleChangeRegistered);
        ScheduleJobRegistered scheduleJobRegistered = scheduleChangeRegistered.getScheduleJobRegistered();
        try {
            ByteBuffer contextValidationRequest = ByteBuffer.wrap(scheduleJobRegistered.getContext());
            ContextValidationResponse contextValidationResponse = validateExecutionContext(scheduleJobRegistered.getExecutorServicePath(), contextValidationRequest);
            ScheduleContextValidated scheduleContextValidated = new ScheduleContextValidated(contextValidationRequest, contextValidationResponse);
            ScheduleChange scheduleChangeValidated = ScheduleChange.schedule_context_validated(scheduleContextValidated);
            ScheduledJobContext scheduledJobContext = getScheduledJobContext(scheduleJobRegistered);
            ComplexAction complexAction = buildComplexAction(scheduledJobContext.getNextFireTime());
            SignalResultData<ScheduleChange> resultData = new SignalResultData<>(Arrays.asList(scheduleChangeRegistered, scheduleChangeValidated), complexAction);
            log.info("Response of processSignalInit: {}", resultData);
            return resultData;
        } catch (Exception e) {
            log.warn("Couldn't processSignalInit, machineId={}, scheduleChangeRegistered={}", machineId, scheduleChangeRegistered, e);
            return new SignalResultData<>(Collections.emptyList(), new ComplexAction());
        }
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
        BusinessSchedule businessSchedule = dominantService.getBusinessSchedule(dominantSchedule.getBusinessScheduleRef(), dominantSchedule.getRevision());
        Calendar calendar = dominantService.getCalendar(dominantSchedule.getCalendarRef(), dominantSchedule.getRevision());
        return getScheduledJobContext(calendar, businessSchedule);
    }

    @Override
    protected SignalResultData<ScheduleChange> processSignalTimeout(String namespace, String machineId, List<TMachineEvent<ScheduleChange>> list) {
        log.info("Request processSignalTimeout() machineId: {} list: {}", machineId, list);
        try {
            ScheduleJobRegistered scheduleJobRegistered = list.stream().filter(e -> e.getData().isSetScheduleJobRegistered()).findFirst()
                    .orElseThrow(() -> new NotFoundException("Couldn't found ScheduleJobRegistered for machineId = " + machineId)).getData().getScheduleJobRegistered();
            String url = scheduleJobRegistered.getExecutorServicePath();
            ScheduledJobExecutorSrv.Iface remoteClient = getRemoteClient(url);
            ExecuteJobRequest executeJobRequest = new ExecuteJobRequest();
            ScheduledJobContext scheduledJobContext = getScheduledJobContext(scheduleJobRegistered);
            executeJobRequest.setScheduledJobContext(scheduledJobContext);
            executeJobRequest.setServiceExecutionContext(scheduleJobRegistered.getContext());
            ByteBuffer genericServiceExecutionContext = remoteClient.executeJob(executeJobRequest);
            ScheduleChange scheduleChange = ScheduleChange.schedule_job_executed(new ScheduleJobExecuted(executeJobRequest, genericServiceExecutionContext));
            ComplexAction complexAction = buildComplexAction(scheduledJobContext.getNextFireTime());
            SignalResultData<ScheduleChange> payoutChangeSignalResultData = new SignalResultData<>(Collections.singletonList(scheduleChange), complexAction);
            log.info("Response of processSignalTimeout: {}", payoutChangeSignalResultData);
            return payoutChangeSignalResultData;
        } catch (Exception e) {
            log.warn("Couldn't processSignalTimeout, machineId={}", machineId, e);
            return new SignalResultData<>(Collections.emptyList(), new ComplexAction());
        }
    }

    @Override
    protected CallResultData<ScheduleChange> processCall(String namespace, String machineId, ScheduleChange scheduleChange, List<TMachineEvent<ScheduleChange>> machineEvents) {
        log.info("Request processCall() machineId: {} scheduleChange: {} machineEvents: {}", machineId, scheduleChange, machineEvents);
        ComplexAction complexAction = new ComplexAction();
        TimerAction timer = new TimerAction();
        timer.setUnsetTimer(new UnsetTimerAction());
        complexAction.setTimer(timer);
        CallResultData<ScheduleChange> callResultData = new CallResultData<>(scheduleChange, Collections.singletonList(scheduleChange), complexAction);
        log.info("Response of processCall: {}", callResultData);
        return callResultData;
    }

}
