package com.rbkmoney.schedulator.handler;

import com.rbkmoney.damsel.schedule.*;
import com.rbkmoney.machinarium.domain.SignalResultData;
import com.rbkmoney.machinarium.domain.TMachine;
import com.rbkmoney.machinarium.domain.TMachineEvent;
import com.rbkmoney.machinegun.msgpack.Nil;
import com.rbkmoney.machinegun.msgpack.Value;
import com.rbkmoney.machinegun.stateproc.ComplexAction;
import com.rbkmoney.schedulator.service.ScheduleJobService;
import com.rbkmoney.schedulator.util.TimerActionHelper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.Collections;

@Slf4j
@Component
public class ScheduleCalculatorMachineEventHandler extends BaseMachineEventHandler<ScheduleChange> {

    private final RemoteClientManager remoteClientManager;

    private final ScheduleJobService scheduleJobService;

    protected ScheduleCalculatorMachineEventHandler(RemoveMachineEventHandler removeMachineEventHandler,
                                                    RemoteClientManager remoteClientManager,
                                                    ScheduleJobService scheduleJobService) {
        super(removeMachineEventHandler);
        this.remoteClientManager = remoteClientManager;
        this.scheduleJobService = scheduleJobService;
    }

    @Override
    protected SignalResultData<ScheduleChange> handleEvent(TMachine<ScheduleChange> machine,
                                                           TMachineEvent<ScheduleChange> machineEvent) throws MachineEventHandleException {
        ScheduleJobRegistered scheduleJobRegistered = machineEvent.getData().getScheduleJobRegistered();
        try {
            log.info("Handle register schedule machine event: {}", scheduleJobRegistered);

            // Calculate next execution time
            ScheduleJobCalculateHolder scheduleJobCalculateHolder = calculateNextExecutionTime(machine, scheduleJobRegistered);

            // Build timeout signal result
            ScheduleChange scheduleChange = ScheduleChange.schedule_job_executed(
                    new ScheduleJobExecuted(scheduleJobCalculateHolder.getExecuteJobRequest(), scheduleJobCalculateHolder.getRemoteJobContext())
            );
            ScheduledJobContext scheduledJobContext = scheduleJobCalculateHolder.getScheduledJobContext();
            ComplexAction complexAction = TimerActionHelper.buildTimerAction(scheduledJobContext.getNextFireTime());

            SignalResultData<ScheduleChange> signalResultData = new SignalResultData<>(
                    Value.nl(new Nil()),
                    Collections.singletonList(scheduleChange),
                    complexAction);
            log.info("Response of processSignalTimeout: {}", signalResultData);

            return signalResultData;
        } catch (Exception e) {
            String errMsg = String.format("Unexpected exception while handle '%s' schedule for '%s'",
                    scheduleJobRegistered.getScheduleId(), scheduleJobRegistered.getExecutorServicePath());
            throw new MachineEventHandleException(errMsg, e);
        }
    }

    @Override
    public boolean canHandle(TMachineEvent<ScheduleChange> machineEvent) {
        return machineEvent.getData().isSetScheduleJobRegistered();
    }

    private ScheduleJobCalculateHolder calculateNextExecutionTime(TMachine<ScheduleChange> machine,
                                                                  ScheduleJobRegistered scheduleJobRegistered) {
        // Calculate execution time
        ExecuteJobRequest executeJobRequest = new ExecuteJobRequest();
        ScheduledJobContext scheduledJobContext = scheduleJobService.calculateScheduledJobContext(scheduleJobRegistered);
        executeJobRequest.setScheduledJobContext(scheduledJobContext);
        executeJobRequest.setServiceExecutionContext(scheduleJobRegistered.getContext());

        String url = scheduleJobRegistered.getExecutorServicePath();
        ByteBuffer remoteJobContext = callRemoteJob(url, executeJobRequest);

        // Calculate retry execution time
        if (remoteJobContext == null) {
            remoteJobContext = ByteBuffer.wrap(scheduleJobRegistered.getContext()); // Set old execution context
            scheduledJobContext = scheduleJobService.calculateRetryJobContext(scheduleJobRegistered, machine.getTimer());
        }

        return new ScheduleJobCalculateHolder(executeJobRequest, scheduledJobContext, remoteJobContext);
    }

    private ByteBuffer callRemoteJob(String url, ExecuteJobRequest executeJobRequest) {
        try {
            ScheduledJobExecutorSrv.Iface remoteClient = remoteClientManager.getRemoteClient(url);
            return remoteClient.executeJob(executeJobRequest);
        } catch (Exception e) {
            log.error("Call '%s' job failed. Set old 'remoteJobContext' variable", e);
            return null;
        }
    }

    @Data
    private static final class ScheduleJobCalculateHolder {

        private final ExecuteJobRequest executeJobRequest;

        private final ScheduledJobContext scheduledJobContext;

        private final ByteBuffer remoteJobContext;

    }

}
