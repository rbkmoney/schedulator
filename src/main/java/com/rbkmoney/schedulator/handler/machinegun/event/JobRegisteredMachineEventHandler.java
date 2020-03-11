package com.rbkmoney.schedulator.handler.machinegun.event;

import com.rbkmoney.damsel.schedule.*;
import com.rbkmoney.machinarium.domain.SignalResultData;
import com.rbkmoney.machinarium.domain.TMachine;
import com.rbkmoney.machinarium.domain.TMachineEvent;
import com.rbkmoney.machinegun.msgpack.Nil;
import com.rbkmoney.machinegun.msgpack.Value;
import com.rbkmoney.machinegun.stateproc.ComplexAction;
import com.rbkmoney.machinegun.stateproc.HistoryRange;
import com.rbkmoney.schedulator.serializer.MachineRegisterState;
import com.rbkmoney.schedulator.serializer.MachineStateSerializer;
import com.rbkmoney.schedulator.serializer.RegisterContext;
import com.rbkmoney.schedulator.serializer.SchedulatorMachineState;
import com.rbkmoney.schedulator.service.RemoteClientManager;
import com.rbkmoney.schedulator.service.ScheduleJobService;
import com.rbkmoney.schedulator.service.model.ScheduleJobCalculateResult;
import com.rbkmoney.schedulator.util.TimerActionHelper;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.Collections;

@Slf4j
@Component
@RequiredArgsConstructor
public class JobRegisteredMachineEventHandler implements MachineEventHandler {

    private final ScheduleJobService scheduleJobService;

    private final MachineStateSerializer machineStateSerializer;

    @Override
    public SignalResultData<ScheduleChange> handleEvent(TMachine<ScheduleChange> machine,
                                                        TMachineEvent<ScheduleChange> event) {
        log.info("Process job register event (time calculation) for machineId: {}", machine.getMachineId());
        ScheduleJobRegistered scheduleJobRegistered = event.getData().getScheduleJobRegistered();
        try {
            log.info("Handle register schedule machine event: {}", scheduleJobRegistered);

            // Calculate next execution time
            ScheduleJobCalculateResult scheduleJobCalculateResult =
                    scheduleJobService.calculateNextExecutionTime(machine, scheduleJobRegistered);

            // Build timeout signal result
            ScheduleChange scheduleChange = ScheduleChange.schedule_job_executed(
                    new ScheduleJobExecuted(scheduleJobCalculateResult.getExecuteJobRequest(), scheduleJobCalculateResult.getRemoteJobContext())
            );
            ScheduledJobContext scheduledJobContext = scheduleJobCalculateResult.getScheduledJobContext();
            HistoryRange historyRange = TimerActionHelper.buildLastEventHistoryRange();
            ComplexAction complexAction = TimerActionHelper.buildTimerAction(
                    scheduledJobContext.getNextFireTime(), historyRange);

            log.info("[ScheduleCalculator] timer action: {}", complexAction);

            SchedulatorMachineState schedulatorMachineState = new SchedulatorMachineState(scheduleJobRegistered);
            byte[] state = machineStateSerializer.serialize(schedulatorMachineState);

            log.info("Schedulator machine state: {}", schedulatorMachineState);

            SignalResultData<ScheduleChange> signalResultData = new SignalResultData<>(
                    Value.bin(state),
                    Collections.singletonList(scheduleChange),
                    complexAction);
            log.info("Response of processSignalTimeout: {}", signalResultData);

            return signalResultData;
        } catch (Exception e) {
            String errMsg = String.format("Unexpected exception during handle '%s' schedule for '%s'",
                    scheduleJobRegistered.getScheduleId(), scheduleJobRegistered.getExecutorServicePath());
            throw new EventHandlerException(errMsg, e);
        }
    }

    @Override
    public boolean isHandle(TMachine<ScheduleChange> machine, TMachineEvent<ScheduleChange> event) {
        return event.getData().isSetScheduleJobRegistered();
    }

}
