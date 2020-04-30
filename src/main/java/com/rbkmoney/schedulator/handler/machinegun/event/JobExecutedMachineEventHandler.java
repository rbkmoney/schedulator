package com.rbkmoney.schedulator.handler.machinegun.event;

import com.rbkmoney.damsel.domain.BusinessScheduleRef;
import com.rbkmoney.damsel.domain.CalendarRef;
import com.rbkmoney.damsel.schedule.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinarium.domain.SignalResultData;
import com.rbkmoney.machinarium.domain.TMachine;
import com.rbkmoney.machinarium.domain.TMachineEvent;
import com.rbkmoney.machinegun.msgpack.Value;
import com.rbkmoney.machinegun.stateproc.ComplexAction;
import com.rbkmoney.machinegun.stateproc.HistoryRange;
import com.rbkmoney.schedulator.serializer.MachineRegisterState;
import com.rbkmoney.schedulator.serializer.MachineStateSerializer;
import com.rbkmoney.schedulator.serializer.MachineTimerState;
import com.rbkmoney.schedulator.serializer.SchedulatorMachineState;
import com.rbkmoney.schedulator.service.ScheduleJobService;
import com.rbkmoney.schedulator.service.model.ScheduleJobCalculateResult;
import com.rbkmoney.schedulator.util.TimerActionHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collections;

@Slf4j
@Component
@RequiredArgsConstructor
public class JobExecutedMachineEventHandler implements MachineEventHandler {

    private final ScheduleJobService scheduleJobService;

    private final MachineStateSerializer machineStateSerializer;

    @Override
    public SignalResultData<ScheduleChange> handleEvent(TMachine<ScheduleChange> machine,
                                                        TMachineEvent<ScheduleChange> event) {
        log.info("Process job executed event for machineId: {}", machine.getMachineId());
        if (machine.getMachineState() == null) {
            throw new IllegalStateException("Machine state can't be null");
        }

        // Read current state
        byte[] state = machine.getMachineState().getData().getBin();
        SchedulatorMachineState schedulatorMachineState = machineStateSerializer.deserializer(state);
        MachineRegisterState registerState = schedulatorMachineState.getRegisterState();
        ScheduleJobRegistered scheduleJobRegistered = mapToScheduleJobRegistered(registerState);
        MachineTimerState timerState = schedulatorMachineState.getTimerState();

        // Calculate next execution
        ScheduleJobCalculateResult scheduleJobCalculateResult =
                scheduleJobService.calculateNextExecutionTime(machine, scheduleJobRegistered, timerState);

        ScheduleChange scheduleChange = ScheduleChange.schedule_job_executed(
                new ScheduleJobExecuted(
                        scheduleJobCalculateResult.getExecuteJobRequest(),
                        scheduleJobCalculateResult.getRemoteJobContext()
                )
        );
        ScheduledJobContext scheduledJobContext = scheduleJobCalculateResult.getScheduledJobContext();
        HistoryRange historyRange = TimerActionHelper.buildLastEventHistoryRange();
        ComplexAction complexAction = TimerActionHelper.buildTimerAction(
                scheduledJobContext.getNextFireTime(), historyRange);

        log.info("Timer action: {}", complexAction);

        // Result machine state
        Instant nextFireTime = TypeUtil.stringToInstant(scheduledJobContext.getNextFireTime());
        schedulatorMachineState.getTimerState().setNextTimer(nextFireTime);
        byte[] resultState = machineStateSerializer.serialize(schedulatorMachineState);

        SignalResultData<ScheduleChange> signalResultData = new SignalResultData<>(
                Value.bin(resultState),
                Collections.singletonList(scheduleChange),
                complexAction);
        log.debug("Response of processSignalTimeout: {}", signalResultData);

        return signalResultData;
    }

    @Override
    public boolean isHandle(TMachine<ScheduleChange> machine, TMachineEvent<ScheduleChange> event) {
        return event.getData().isSetScheduleJobExecuted();
    }

    private ScheduleJobRegistered mapToScheduleJobRegistered(MachineRegisterState registerState) {
        DominantBasedSchedule dominantBasedSchedule = new DominantBasedSchedule()
                .setBusinessScheduleRef(new BusinessScheduleRef(registerState.getBusinessSchedulerId()))
                .setCalendarRef(new CalendarRef(registerState.getCalendarId()))
                .setRevision(registerState.getDominantRevisionId());
        Schedule schedule = Schedule.dominant_schedule(dominantBasedSchedule);
        return new ScheduleJobRegistered()
                .setContext(registerState.getContext().getBytes())
                .setExecutorServicePath(registerState.getExecutorServicePath())
                .setScheduleId(registerState.getSchedulerId())
                .setSchedule(schedule);
    }

}
