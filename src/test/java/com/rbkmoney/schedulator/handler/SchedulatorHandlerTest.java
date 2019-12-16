package com.rbkmoney.schedulator.handler;

import com.rbkmoney.damsel.domain.BusinessScheduleRef;
import com.rbkmoney.damsel.domain.CalendarRef;
import com.rbkmoney.damsel.schedule.*;
import com.rbkmoney.machinarium.client.AutomatonClient;
import com.rbkmoney.machinarium.domain.TMachineEvent;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SchedulatorHandlerTest {

    @MockBean
    private AutomatonClient<ScheduleChange, ScheduleChange> automatonClientMock;

    @Autowired
    private SchedulatorSrv.Iface schedulator;

    @Test
    public void registerJobSuccessTest() throws TException {
        ArgumentCaptor<ScheduleChange> scheduleChangeArgumentCaptor = ArgumentCaptor.forClass(ScheduleChange.class);

        doNothing().when(automatonClientMock).start(anyString(), scheduleChangeArgumentCaptor.capture());

        when(automatonClientMock.getEvents(anyString())).thenReturn(buildValidEvents());

        RegisterJobRequest registerJobRequest = buildRegisterJob();

        schedulator.registerJob("64", registerJobRequest);

        ScheduleChange scheduleChange = scheduleChangeArgumentCaptor.getValue();

        Assert.assertEquals(registerJobRequest.getExecutorServicePath(), scheduleChange.getScheduleJobRegistered().getExecutorServicePath());
        Assert.assertEquals(registerJobRequest.getSchedule().getDominantSchedule().getRevision(),
                scheduleChange.getScheduleJobRegistered().getSchedule().getDominantSchedule().getRevision());
        Assert.assertEquals(registerJobRequest.getSchedule().getDominantSchedule().getBusinessScheduleRef().getId(),
                scheduleChange.getScheduleJobRegistered().getSchedule().getDominantSchedule().getBusinessScheduleRef().getId());
        Assert.assertEquals(registerJobRequest.getSchedule().getDominantSchedule().getCalendarRef().getId(),
                scheduleChange.getScheduleJobRegistered().getSchedule().getDominantSchedule().getCalendarRef().getId());
    }

    @Test(expected = IllegalStateException.class)
    public void registerJobBadEventTest() throws TException {
        when(automatonClientMock.getEvents(anyString())).thenReturn(buildBadEvents());

        RegisterJobRequest registerJobRequest = buildRegisterJob();
        schedulator.registerJob("64", registerJobRequest);
    }

    private List<TMachineEvent<ScheduleChange>> buildValidEvents() {
        ScheduleChange registerScheduleChange = new ScheduleChange();
        registerScheduleChange.setScheduleJobRegistered(new ScheduleJobRegistered());
        TMachineEvent<ScheduleChange> registerEvent = new TMachineEvent<>(64, Instant.now(), registerScheduleChange);

        ScheduleChange validateScheduleChange = new ScheduleChange();
        validateScheduleChange.setScheduleContextValidated(
                new ScheduleContextValidated().setResponse(
                        new ContextValidationResponse().setResponseStatus(new ValidationResponseStatus())
                )
        );
        TMachineEvent<ScheduleChange> validateEvent = new TMachineEvent<>(65, Instant.now(), validateScheduleChange);

        return List.of(registerEvent, validateEvent);
    }

    private List<TMachineEvent<ScheduleChange>> buildBadEvents() {
        ScheduleChange registerScheduleChange = new ScheduleChange();
        registerScheduleChange.setScheduleJobRegistered(new ScheduleJobRegistered());
        TMachineEvent<ScheduleChange> registerEvent = new TMachineEvent<>(64, Instant.now(), registerScheduleChange);

        return List.of(registerEvent);
    }

    private RegisterJobRequest buildRegisterJob() {
        DominantBasedSchedule dominantBasedSchedule = new DominantBasedSchedule()
                .setBusinessScheduleRef(new BusinessScheduleRef(64))
                .setRevision(12435)
                .setCalendarRef(new CalendarRef(1));

        Schedule schedule = new Schedule();
        schedule.setDominantSchedule(dominantBasedSchedule);

        return new RegisterJobRequest()
                .setExecutorServicePath("testExecitorServicePath")
                .setContext(new byte[0])
                .setSchedule(schedule);
    }

}
