package com.rbkmoney.schedulator.handler.aspect;

import com.rbkmoney.damsel.schedule.ScheduleChange;
import com.rbkmoney.damsel.schedule.ScheduleJobRegistered;
import com.rbkmoney.geck.serializer.Geck;
import com.rbkmoney.machinarium.domain.TMachineEvent;
import com.rbkmoney.machinarium.util.TMachineUtil;
import com.rbkmoney.machinegun.stateproc.Signal;
import com.rbkmoney.machinegun.stateproc.SignalArgs;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Aspect
@Component
public class MqProcessorMdcAspect {

    @Around("execution(* com.rbkmoney.machinarium.handler.AbstractProcessorHandler+.processSignal(*))" +
            " && target(com.rbkmoney.schedulator.handler.MgProcessorHandler)")
    public Object mdcLogger(ProceedingJoinPoint pjp) throws Throwable {
        try {
            SignalArgs signalArgs = (SignalArgs) pjp.getArgs()[0];
            Signal._Fields signalType = signalArgs.getSignal().getSetField();
            if (signalType == Signal._Fields.INIT) {
                processInitSignal(signalArgs);
            } else if (signalType == Signal._Fields.TIMEOUT) {
                processSignalTimeout(signalArgs);
            }
            return pjp.proceed();
        } finally {
            MDC.clear();
        }
    }

    private void processInitSignal(SignalArgs signalArgs) {
        byte[] bin = signalArgs.getSignal().getInit().getArg().getBin();
        ScheduleChange scheduleChange = Geck.msgPackToTBase(bin, ScheduleChange.class);
        MDC.put("machine_id", scheduleChange.getScheduleJobRegistered().getScheduleId());
    }

    private void processSignalTimeout(SignalArgs signalArgs) {
        List<TMachineEvent<ScheduleChange>> machineEvents = TMachineUtil.getMachineEvents(signalArgs.getMachine(), ScheduleChange.class);
        Optional<TMachineEvent<ScheduleChange>> scheduleJobRegisteredEvent = machineEvents.stream()
                .filter(machineEvent -> machineEvent.getData().isSetScheduleJobRegistered())
                .findFirst();
        if (scheduleJobRegisteredEvent.isPresent()) {
            ScheduleJobRegistered scheduleJobRegistered = scheduleJobRegisteredEvent.get().getData().getScheduleJobRegistered();
            MDC.put("machine_id", scheduleJobRegistered.getScheduleId());
        }
    }

}
