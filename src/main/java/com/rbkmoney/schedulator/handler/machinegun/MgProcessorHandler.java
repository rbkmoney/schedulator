package com.rbkmoney.schedulator.handler.machinegun;

import com.rbkmoney.damsel.schedule.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinarium.domain.CallResultData;
import com.rbkmoney.machinarium.domain.SignalResultData;
import com.rbkmoney.machinarium.domain.TMachine;
import com.rbkmoney.machinarium.domain.TMachineEvent;
import com.rbkmoney.machinarium.handler.AbstractProcessorHandler;
import com.rbkmoney.machinegun.msgpack.Nil;
import com.rbkmoney.machinegun.msgpack.Value;
import com.rbkmoney.machinegun.stateproc.ComplexAction;
import com.rbkmoney.machinegun.stateproc.HistoryRange;
import com.rbkmoney.schedulator.handler.machinegun.event.MachineEventHandler;
import com.rbkmoney.schedulator.handler.machinegun.event.MachineEventProcessor;
import com.rbkmoney.schedulator.service.RemoteClientManager;
import com.rbkmoney.schedulator.service.ScheduleJobService;
import com.rbkmoney.schedulator.service.impl.ScheduleJobCalculateException;
import com.rbkmoney.schedulator.util.TimerActionHelper;
import com.rbkmoney.woody.api.flow.error.WUnavailableResultException;
import com.rbkmoney.woody.api.flow.error.WUndefinedResultException;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Slf4j
@Component
public class MgProcessorHandler extends AbstractProcessorHandler<ScheduleChange, ScheduleChange> {

    private final RemoteClientManager remoteClientManager;

    private final ScheduleJobService scheduleJobService;

    private final List<MachineEventHandler> machineEventHandlers;

    private final MachineEventProcessor machineEventProcessor;

    public MgProcessorHandler(RemoteClientManager remoteClientManager,
                              ScheduleJobService scheduleJobService,
                              List<MachineEventHandler> machineEventHandlers,
                              MachineEventProcessor machineEventProcessor) {
        super(ScheduleChange.class, ScheduleChange.class);
        this.remoteClientManager = remoteClientManager;
        this.scheduleJobService = scheduleJobService;
        this.machineEventHandlers = machineEventHandlers;
        this.machineEventProcessor = machineEventProcessor;
    }

    @Override
    public final SignalResultData<ScheduleChange> processSignalInit(TMachine machine, ScheduleChange scheduleChangeRegistered) {
        log.info("Request processSignalInit() machineId: {} scheduleChangeRegistered: {}", machine.getMachineId(), scheduleChangeRegistered);
        ScheduleJobRegistered scheduleJobRegistered = scheduleChangeRegistered.getScheduleJobRegistered();

        // Validate execution context (call remote service)
        ScheduleContextValidated scheduleContextValidated = validateRemoteContext(scheduleJobRegistered);

        // Calculate next execution time
        try {
            ScheduleChange scheduleChangeValidated = ScheduleChange.schedule_context_validated(scheduleContextValidated);
            ScheduledJobContext scheduledJobContext = scheduleJobService.calculateScheduledJobContext(scheduleJobRegistered);
            HistoryRange historyRange = TimerActionHelper.buildLastEventHistoryRange();
            ComplexAction complexAction = TimerActionHelper.buildTimerAction(scheduledJobContext.getNextFireTime(), historyRange);
            log.info("[Signal Init] timer action: {}", complexAction);
            SignalResultData<ScheduleChange> signalResultData = new SignalResultData<>(
                    Value.nl(new Nil()),
                    Arrays.asList(scheduleChangeValidated, scheduleChangeRegistered),
                    complexAction);
            log.info("Response of processSignalInit: {}", signalResultData);

            return signalResultData;
        } catch (ScheduleJobCalculateException e) {
            log.error("Failed to calculate schedule", e);
            throw new WUndefinedResultException("Failed to calculate schedule", e);
        }
    }

    @Override
    public final SignalResultData<ScheduleChange> processSignalTimeout(TMachine<ScheduleChange> machine,
                                                                       List<TMachineEvent<ScheduleChange>> machineEventList) {
        try {
            if (machineEventList.isEmpty()) {
                throw new MachineEventHandleException("Machine events can't be empty");
            }
            TMachineEvent<ScheduleChange> machineEvent = machineEventList.get(0); // Expect only one event

            log.info("Request processSignalTimeout() machineId: {} machineEventList: {}", machine.getMachineId(), machineEventList);

            return machineEventProcessor.process(machine, machineEvent);
        } catch (MachineEventHandleException e) {
            log.error("Exception while handle event for machineId = {}", machine, e);
            throw new WUndefinedResultException(e);
        }
    }

    @Override
    public final CallResultData<ScheduleChange> processCall(String namespace,
                                                            String machineId,
                                                            ScheduleChange scheduleChange,
                                                            List<TMachineEvent<ScheduleChange>> machineEvents) {
        log.info("Request processCall() machineId: {} scheduleChange: {} machineEvents: {}", machineId, scheduleChange, machineEvents);
        ComplexAction removeAction = TimerActionHelper.buildRemoveAction();
        CallResultData<ScheduleChange> callResultData = new CallResultData<>(
                Value.nl(new Nil()),
                scheduleChange,
                Collections.singletonList(scheduleChange),
                removeAction);
        log.info("Response of processCall: {}", callResultData);
        return callResultData;
    }

    private ScheduleContextValidated validateRemoteContext(ScheduleJobRegistered scheduleJobRegistered) {
        try {
            ByteBuffer contextValidationRequest = ByteBuffer.wrap(scheduleJobRegistered.getContext());
            log.info("Call validation context for '{}'", scheduleJobRegistered.getExecutorServicePath());
            ContextValidationResponse contextValidationResponse = remoteClientManager.validateExecutionContext(
                    scheduleJobRegistered.getExecutorServicePath(), contextValidationRequest);
            log.info("Context validation response: {}", contextValidationResponse);

            return new ScheduleContextValidated(contextValidationRequest, contextValidationResponse);
        } catch (WUnavailableResultException e) {
            log.error("Exception while 'validateExecutionContext'. Failed to call remote service", e);
            throw e;
        } catch (TException e) {
            log.error("Unexpected exception while 'validateExecutionContext'");
            throw new WUndefinedResultException(e);
        }
    }

}
