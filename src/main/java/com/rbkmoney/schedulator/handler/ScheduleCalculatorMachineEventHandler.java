package com.rbkmoney.schedulator.handler;

import com.rbkmoney.damsel.schedule.*;
import com.rbkmoney.machinarium.domain.SignalResultData;
import com.rbkmoney.machinarium.domain.TMachineEvent;
import com.rbkmoney.machinegun.msgpack.Nil;
import com.rbkmoney.machinegun.msgpack.Value;
import com.rbkmoney.machinegun.stateproc.ComplexAction;
import com.rbkmoney.schedulator.service.ScheduleJobService;
import com.rbkmoney.schedulator.util.TimerActionHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
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
    protected SignalResultData<ScheduleChange> handleEvent(TMachineEvent<ScheduleChange> machineEvent) throws MachineEventHandleException {
        try {
            ScheduleJobRegistered scheduleJobRegistered = machineEvent.getData().getScheduleJobRegistered();

            String url = scheduleJobRegistered.getExecutorServicePath();

            // Calculate next execution time
            ExecuteJobRequest executeJobRequest = new ExecuteJobRequest();
            ScheduledJobContext scheduledJobContext = scheduleJobService.getScheduledJobContext(scheduleJobRegistered);
            executeJobRequest.setScheduledJobContext(scheduledJobContext);
            executeJobRequest.setServiceExecutionContext(scheduleJobRegistered.getContext());

            // Execute remote client
            log.info("Execute job for '{}'", url);
            ScheduledJobExecutorSrv.Iface remoteClient = remoteClientManager.getRemoteClient(url);
            ByteBuffer genericServiceExecutionContext = remoteClient.executeJob(executeJobRequest);

            // Build timeout signal result
            ScheduleChange scheduleChange = ScheduleChange.schedule_job_executed(
                    new ScheduleJobExecuted(executeJobRequest, genericServiceExecutionContext)
            );
            ComplexAction complexAction = TimerActionHelper.buildTimerAction(scheduledJobContext.getNextFireTime());

            SignalResultData<ScheduleChange> signalResultData = new SignalResultData<>(
                    Value.nl(new Nil()),
                    Collections.singletonList(scheduleChange),
                    complexAction);
            log.info("Response of processSignalTimeout: {}", signalResultData);

            return signalResultData;
        } catch (TException e) {
            throw new MachineEventHandleException("Failed to handle timer job", e);
        }

    }

    @Override
    public boolean canHandle(TMachineEvent<ScheduleChange> machineEvent) {
        return machineEvent.getData().isSetScheduleJobRegistered();
    }

}
