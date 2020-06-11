package com.rbkmoney.schedulator.handler.machinegun;

import com.rbkmoney.damsel.schedule.ScheduleChange;
import com.rbkmoney.damsel.schedule.ScheduleJobRegistered;
import com.rbkmoney.machinarium.domain.CallResultData;
import com.rbkmoney.machinarium.domain.SignalResultData;
import com.rbkmoney.machinarium.domain.TMachine;
import com.rbkmoney.machinarium.domain.TMachineEvent;
import com.rbkmoney.machinarium.handler.AbstractProcessorHandler;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

import java.util.List;
import java.util.Optional;

@Slf4j
public class MgProcessorMdcDecorator extends AbstractProcessorHandler<ScheduleChange, ScheduleChange> {

    private static final String MACHINE_ID = "machine_id";

    private final MgProcessorHandler mgProcessorHandler;

    public MgProcessorMdcDecorator(MgProcessorHandler mgProcessorHandler) {
        super(ScheduleChange.class, ScheduleChange.class);
        this.mgProcessorHandler = mgProcessorHandler;
    }

    @Override
    protected SignalResultData<ScheduleChange> processSignalInit(TMachine<ScheduleChange> machine, ScheduleChange args) {
        try {
            if (args.isSetScheduleJobRegistered()) {
                MDC.put(MACHINE_ID, args.getScheduleJobRegistered().getScheduleId());
            }
            return mgProcessorHandler.processSignalInit(machine, args);
        } finally {
            MDC.clear();
        }
    }

    @Override
    protected SignalResultData<ScheduleChange> processSignalTimeout(TMachine<ScheduleChange> machine,
                                                                 List<TMachineEvent<ScheduleChange>> machineEvents) {
        try {
            Optional<TMachineEvent<ScheduleChange>> scheduleJobRegisteredEvent = machineEvents.stream()
                    .filter(machineEvent -> machineEvent.getData().isSetScheduleJobRegistered())
                    .findFirst();
            if (scheduleJobRegisteredEvent.isPresent()) {
                ScheduleJobRegistered scheduleJobRegistered = scheduleJobRegisteredEvent.get().getData().getScheduleJobRegistered();
                MDC.put(MACHINE_ID, scheduleJobRegistered.getScheduleId());
            }
            return mgProcessorHandler.processSignalTimeout(machine, machineEvents);
        } finally {
            MDC.clear();
        }

    }

    @Override
    protected CallResultData<ScheduleChange> processCall(String namespace,
                                                         String machineId,
                                                         ScheduleChange args,
                                                         List<TMachineEvent<ScheduleChange>> machineEvents) {
        return mgProcessorHandler.processCall(namespace, machineId, args, machineEvents);
    }
}
