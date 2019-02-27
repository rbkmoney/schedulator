package com.rbkmoney.schedulator.handler;

import com.rbkmoney.damsel.schedule.*;
import com.rbkmoney.machinarium.client.AutomatonClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SchedulatorHandler implements SchedulatorSrv.Iface{

    private final AutomatonClient<ScheduleChange, ScheduleChange> automatonClient;

    @Override
    public void registerJob(String scheduleId, RegisterJobRequest registerJobRequest) throws ScheduleAlreadyExists, BadContextProvided, TException {
        ScheduleJobRegistered jobRegistered = new ScheduleJobRegistered()
                .setExecutorServicePath(registerJobRequest.getExecutorServicePath())
                .setContext(registerJobRequest.getContext());

        if (registerJobRequest.getSchedule().isSetDominantSchedule()) {
            DominantBasedSchedule dominantSchedule = registerJobRequest.getSchedule().getDominantSchedule();
            jobRegistered.setSchedule(Schedule.dominant_schedule(new DominantBasedSchedule()
                    .setBusinessScheduleRef(dominantSchedule.getBusinessScheduleRef())
                    .setCalendarRef(dominantSchedule.getCalendarRef())
                    .setRevision(dominantSchedule.getRevision())));
        }

        ScheduleChange scheduleChange = ScheduleChange.schedule_job_registered(jobRegistered);
        automatonClient.start(scheduleId, scheduleChange);
    }

    @Override
    public void deregisterJob(String scheduleId) throws ScheduleNotFound, TException {
        automatonClient.call(scheduleId, ScheduleChange.schedule_job_deregistered(new ScheduleJobDeregistered()));
    }
}
