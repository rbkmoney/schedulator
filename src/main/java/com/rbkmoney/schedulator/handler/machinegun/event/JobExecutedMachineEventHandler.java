package com.rbkmoney.schedulator.handler.machinegun.event;

import com.rbkmoney.damsel.schedule.ScheduleChange;
import com.rbkmoney.damsel.schedule.ScheduleJobExecuted;
import com.rbkmoney.damsel.schedule.ScheduleJobRegistered;
import com.rbkmoney.damsel.schedule.ScheduledJobContext;
import com.rbkmoney.machinarium.domain.SignalResultData;
import com.rbkmoney.machinarium.domain.TMachine;
import com.rbkmoney.machinarium.domain.TMachineEvent;
import com.rbkmoney.machinegun.msgpack.Value;
import com.rbkmoney.machinegun.stateproc.ComplexAction;
import com.rbkmoney.machinegun.stateproc.HistoryRange;
import com.rbkmoney.schedulator.serializer.MachineRegisterState;
import com.rbkmoney.schedulator.serializer.MachineStateSerializer;
import com.rbkmoney.schedulator.serializer.SchedulatorMachineState;
import com.rbkmoney.schedulator.service.ScheduleJobService;
import com.rbkmoney.schedulator.service.model.ScheduleJobCalculateResult;
import com.rbkmoney.schedulator.util.TimerActionHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collections;

@Slf4j
@Component
@RequiredArgsConstructor
public class JobExecutedMachineEventHandler implements MachineEventHandler {

    private final ScheduleJobService scheduleJobService;

    private final MachineStateSerializer machineStateSerializer;

    @Override
    public SignalResultData<ScheduleChange> handleEvent(TMachine<ScheduleChange> machine,
                                                        TMachineEvent<ScheduleChange> event,
                                                        DefaultMachineEventChain filterChain) {
        if (!event.getData().isSetScheduleJobExecuted()) {
            return filterChain.processEventChain(machine, event);
        }

        log.info("Process job executed event for machineId: {}", machine.getMachineId());
        if (machine.getMachineState() == null) {
            throw new IllegalStateException("Machine state can't be null");
        }

        // Read current state
        byte[] state = machine.getMachineState().getData().getBin();
        SchedulatorMachineState schedulatorMachineState = machineStateSerializer.deserializer(state);
        MachineRegisterState registerState = schedulatorMachineState.getRegisterState();
        ScheduleJobRegistered scheduleJobRegistered = registerState.mapToScheduleJobRegistered();

        // Calculate next execution
        ScheduleJobCalculateResult scheduleJobCalculateResult =
                scheduleJobService.calculateNextExecutionTime(machine, scheduleJobRegistered);

        ScheduleChange scheduleChange = ScheduleChange.schedule_job_executed(
                new ScheduleJobExecuted(scheduleJobCalculateResult.getExecuteJobRequest(), scheduleJobCalculateResult.getRemoteJobContext())
        );
        ScheduledJobContext scheduledJobContext = scheduleJobCalculateResult.getScheduledJobContext();
        HistoryRange historyRange = TimerActionHelper.buildLastEventHistoryRange();
        ComplexAction complexAction = TimerActionHelper.buildTimerAction(
                scheduledJobContext.getNextFireTime(), historyRange);

        SignalResultData<ScheduleChange> signalResultData = new SignalResultData<>(
                Value.bin(state),
                Collections.singletonList(scheduleChange),
                complexAction);
        log.info("Response of processSignalTimeout: {}", signalResultData);

        return signalResultData;
    }
}
