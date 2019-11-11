package com.rbkmoney.schedulator.handler;

import com.rbkmoney.damsel.schedule.*;
import com.rbkmoney.machinarium.client.AutomatonClient;
import com.rbkmoney.machinarium.domain.TMachineEvent;
import com.rbkmoney.machinarium.exception.MachineAlreadyExistsException;
import com.rbkmoney.machinarium.exception.MachineNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class SchedulatorHandler implements SchedulatorSrv.Iface {

    private final AutomatonClient<ScheduleChange, ScheduleChange> automatonClient;

    @Override
    public void registerJob(String scheduleId, RegisterJobRequest registerJobRequest) throws ScheduleAlreadyExists, BadContextProvided, TException {
        log.info("Register job started: scheduleId {}, registerJobRequest {}", scheduleId, registerJobRequest);
        ScheduleJobRegistered jobRegistered = new ScheduleJobRegistered()
                .setScheduleId(scheduleId)
                .setExecutorServicePath(registerJobRequest.getExecutorServicePath())
                .setContext(registerJobRequest.getContext());

        if (registerJobRequest.getSchedule().isSetDominantSchedule()) {
            DominantBasedSchedule dominantSchedule = registerJobRequest.getSchedule().getDominantSchedule();
            jobRegistered.setSchedule(Schedule.dominant_schedule(new DominantBasedSchedule()
                    .setBusinessScheduleRef(dominantSchedule.getBusinessScheduleRef())
                    .setCalendarRef(dominantSchedule.getCalendarRef())
                    .setRevision(dominantSchedule.getRevision())));
        } else {
            throw new BadContextProvided();
        }

        ScheduleChange scheduleChange = ScheduleChange.schedule_job_registered(jobRegistered);
        try {
            automatonClient.start(scheduleId, scheduleChange);
        } catch (MachineAlreadyExistsException e) {
            throw new ScheduleAlreadyExists();
        }

        List<TMachineEvent<ScheduleChange>> events = automatonClient.getEvents(scheduleId);
        if (isScheduleContextValidated(events)) {
            throw new IllegalStateException("Incorrect state of machine " + scheduleId);
        }

        ContextValidationResponse response = events.get(1).getData().getScheduleContextValidated().getResponse();
        if (response.isSetErrors()) {
            if (!response.getErrors().isEmpty()) {
                throw new BadContextProvided(response);
            }
        }

        log.info("Job with scheduleId {} successfully registered", scheduleId);
    }

    @Override
    public void deregisterJob(String scheduleId) throws ScheduleNotFound, TException {
        log.info("Deregister job started: scheduleId {}", scheduleId);
        try {
            automatonClient.call(scheduleId, ScheduleChange.schedule_job_deregistered(new ScheduleJobDeregistered()));
        } catch (MachineNotFoundException e) {
            throw new ScheduleNotFound();
        }
        log.info("Job with scheduleId {} successfully deregistered", scheduleId);
    }

    private boolean isScheduleContextValidated(List<TMachineEvent<ScheduleChange>> events) {
        return events.size() == 2 || events.get(1).getData().isSetScheduleContextValidated();
    }
}
