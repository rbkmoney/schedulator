package com.rbkmoney.schedulator.handler;

import com.rbkmoney.damsel.schedule.*;
import com.rbkmoney.machinarium.domain.CallResultData;
import com.rbkmoney.machinarium.domain.SignalResultData;
import com.rbkmoney.machinarium.domain.TMachineEvent;
import com.rbkmoney.machinarium.handler.AbstractProcessorHandler;
import com.rbkmoney.machinegun.msgpack.Nil;
import com.rbkmoney.machinegun.msgpack.Value;
import com.rbkmoney.machinegun.stateproc.ComplexAction;
import com.rbkmoney.machinegun.stateproc.Content;
import com.rbkmoney.machinegun.stateproc.TimerAction;
import com.rbkmoney.machinegun.stateproc.UnsetTimerAction;
import com.rbkmoney.schedulator.exception.NotFoundException;
import com.rbkmoney.schedulator.service.ScheduleJobService;
import com.rbkmoney.schedulator.util.TimerActionHelper;
import com.rbkmoney.woody.api.flow.error.WUnavailableResultException;
import com.rbkmoney.woody.api.flow.error.WUndefinedResultException;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
@Component
public class MgProcessorHandler extends AbstractProcessorHandler<ScheduleChange, ScheduleChange> {

    private final RemoteClientManager remoteClientManager;

    private final ScheduleJobService scheduleJobService;

    private final ScheduleCalculatorMachineEventHandler scheduleCalculatorMachineEventHandler;

    public MgProcessorHandler(RemoteClientManager remoteClientManager,
                              ScheduleJobService scheduleJobService,
                              ScheduleCalculatorMachineEventHandler scheduleCalculatorMachineEventHandler) {
        super(ScheduleChange.class, ScheduleChange.class);
        this.remoteClientManager = remoteClientManager;
        this.scheduleJobService = scheduleJobService;
        this.scheduleCalculatorMachineEventHandler = scheduleCalculatorMachineEventHandler;
    }

    @Override
    protected SignalResultData<ScheduleChange> processSignalInit(String namespace,
                                                                 String machineId,
                                                                 Content machineState,
                                                                 ScheduleChange scheduleChangeRegistered) {
        log.info("Request processSignalInit() machineId: {} scheduleChangeRegistered: {}", machineId, scheduleChangeRegistered);
        ScheduleJobRegistered scheduleJobRegistered = scheduleChangeRegistered.getScheduleJobRegistered();

        // Validate execution context (call remote service)
        ScheduleContextValidated scheduleContextValidated = validateRemoteContext(scheduleJobRegistered);

        // Calculate next execution time
        ScheduleChange scheduleChangeValidated = ScheduleChange.schedule_context_validated(scheduleContextValidated);
        ScheduledJobContext scheduledJobContext = scheduleJobService.getScheduledJobContext(scheduleJobRegistered);
        ComplexAction complexAction = TimerActionHelper.buildTimerAction(scheduledJobContext.getNextFireTime());
        log.info("Timer action: {}", complexAction);
        SignalResultData<ScheduleChange> signalResultData = new SignalResultData<>(
                Value.nl(new Nil()),
                Arrays.asList(scheduleChangeRegistered, scheduleChangeValidated),
                complexAction);
        log.info("Response of processSignalInit: {}", signalResultData);

        return signalResultData;
    }

    @Override
    protected SignalResultData<ScheduleChange> processSignalTimeout(String namespace,
                                                                    String machineId,
                                                                    Content machineState,
                                                                    List<TMachineEvent<ScheduleChange>> machineEventList) {
        log.info("Request processSignalTimeout() machineId: {} machineEventList: {}", machineId, machineEventList);

        // Search deregister event
        Optional<TMachineEvent<ScheduleChange>> scheduleJobDeregisteredEventOptional = machineEventList.stream()
                .filter(machineEvent -> machineEvent.getData().isSetScheduleJobDeregistered())
                .findFirst();

        // Handle deregister event
        if (scheduleJobDeregisteredEventOptional.isPresent()) {
            log.info("Process job deregister event for machineId: {}", machineId);
            return processEvent(machineId, scheduleJobDeregisteredEventOptional.get());
        }

        // Search register event
        TMachineEvent<ScheduleChange> scheduleJobRegisteredEvent = machineEventList.stream()
                .filter(machineEvent -> machineEvent.getData().isSetScheduleJobRegistered())
                .findFirst()
                .orElseThrow(() -> new NotFoundException("Couldn't found ScheduleJobRegistered for machineId = " + machineId));

        // Handle register event
        log.info("Process job register event for machineId: {}", machineId);
        return processEvent(machineId, scheduleJobRegisteredEvent);
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

    private ScheduleContextValidated validateRemoteContext(ScheduleJobRegistered scheduleJobRegistered) {
        try {
            ByteBuffer contextValidationRequest = ByteBuffer.wrap(scheduleJobRegistered.getContext());
            log.info("Call validation context for '{}'", scheduleJobRegistered.getExecutorServicePath());
            ContextValidationResponse contextValidationResponse = validateExecutionContext(scheduleJobRegistered.getExecutorServicePath(), contextValidationRequest);
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

    private SignalResultData<ScheduleChange> processEvent(String machineId, TMachineEvent<ScheduleChange> event) {
        try {
            return scheduleCalculatorMachineEventHandler.handle(event);
        } catch (MachineEventHandleException e) {
            log.error("Exception while handle event for machineId = {}", machineId, e);
            throw new WUndefinedResultException(e);
        }
    }

    private ContextValidationResponse validateExecutionContext(String url, ByteBuffer context) throws TException {
        ScheduledJobExecutorSrv.Iface client = remoteClientManager.getRemoteClient(url);
        try {
            return client.validateExecutionContext(context);
        } catch (Exception e) {
            throw new WUnavailableResultException(e);
        }
    }

}
