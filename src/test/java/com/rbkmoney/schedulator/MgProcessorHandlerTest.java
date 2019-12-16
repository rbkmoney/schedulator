package com.rbkmoney.schedulator;

import com.rbkmoney.damsel.base.DayOfWeek;
import com.rbkmoney.damsel.base.Month;
import com.rbkmoney.damsel.domain.BusinessSchedule;
import com.rbkmoney.damsel.domain.BusinessScheduleRef;
import com.rbkmoney.damsel.domain.Calendar;
import com.rbkmoney.damsel.domain.CalendarRef;
import com.rbkmoney.damsel.schedule.*;
import com.rbkmoney.geck.serializer.Geck;
import com.rbkmoney.machinegun.msgpack.Value;
import com.rbkmoney.machinegun.stateproc.*;
import com.rbkmoney.schedulator.exception.NotFoundException;
import com.rbkmoney.schedulator.handler.RemoteClientManager;
import com.rbkmoney.schedulator.service.DominantService;
import com.rbkmoney.schedulator.service.ScheduleJobService;
import com.rbkmoney.schedulator.util.SchedulerUtilTest;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MgProcessorHandlerTest {

    @MockBean
    private DominantService dominantServiceMock;

    @MockBean
    private RemoteClientManager remoteClientManagerMock;

    @MockBean
    private ScheduleJobService scheduleJobService;

    @Autowired
    private ProcessorSrv.Iface mqProcessorHandler;

    @Before
    public void setUp() throws Exception {
        ScheduledJobContext scheduledJobContext = new ScheduledJobContext()
                .setNextCronTime("testCron")
                .setNextFireTime("31542524")
                .setPrevFireTime("215426536");
        when(scheduleJobService.getScheduledJobContext(any(ScheduleJobRegistered.class))).thenReturn(scheduledJobContext);

        ScheduledJobExecutorSrv.Iface jobExecutorMock = mock(ScheduledJobExecutorSrv.Iface.class);
        ContextValidationResponse validationResponse = new ContextValidationResponse();
        ValidationResponseStatus validationResponseStatus = new ValidationResponseStatus();
        validationResponseStatus.setSuccess(new ValidationSuccess());
        validationResponse.setResponseStatus(validationResponseStatus);
        when(jobExecutorMock.validateExecutionContext(any(ByteBuffer.class))).thenReturn(validationResponse);
        when(jobExecutorMock.executeJob(any(ExecuteJobRequest.class))).thenReturn(ByteBuffer.wrap(new byte[0]));
        when(remoteClientManagerMock.getRemoteClient(anyString())).thenReturn(jobExecutorMock);

        BusinessSchedule schedule = SchedulerUtilTest.buildSchedule(2018, Month.Apr, (byte) 4, DayOfWeek.Fri, (byte) 7, null, null);

        when(dominantServiceMock.getBusinessSchedule(any(BusinessScheduleRef.class), anyLong())).thenReturn(schedule);

        Calendar calendar = SchedulerUtilTest.buildTestCalendar();

        when(dominantServiceMock.getCalendar(any(CalendarRef.class), anyLong())).thenReturn(calendar);
    }

    @Test
    public void processSignalInitTest() throws TException {
        SignalArgs signalInit = buildSignalInit();
        SignalResult signalResult = mqProcessorHandler.processSignal(signalInit);

        Assert.assertEquals("Machine events should be equal to '2'", 2, signalResult.getChange().getEvents().size());
        Assert.assertTrue("Machine action should be 'timerAction'", signalResult.getAction().isSetTimer());
    }

    @Test
    public void processSignalTimeoutRegisterTest() throws TException {
        SignalArgs signalTimeoutRegister = buildSignalTimeoutRegister();
        SignalResult signalResult = mqProcessorHandler.processSignal(signalTimeoutRegister);

        Assert.assertEquals("Machine events should be equal to '1'", 1, signalResult.getChange().getEvents().size());
        Assert.assertTrue("Machine action should be 'timerAction'", signalResult.getAction().isSetTimer());
    }

    @Test
    public void processSignalTimeoutDeregisterTest() throws TException {
        SignalArgs signalTimeout = buildSignalTimeoutDeregister();
        SignalResult signalResult = mqProcessorHandler.processSignal(signalTimeout);

        Assert.assertEquals("Machine events should be equal to '1'", 1, signalResult.getChange().getEvents().size());
        Assert.assertTrue("Machine action should be 'removeAction'", signalResult.getAction().isSetRemove());
    }

    @Test(expected = NotFoundException.class)
    public void procesSignalNoRegisterEventTest() throws TException {
        SignalArgs signalTimeoutValidated = buildSignalTimeoutValidated();
        SignalResult signalResult = mqProcessorHandler.processSignal(signalTimeoutValidated);
    }

    private SignalArgs buildSignalTimeoutRegister() {
        ScheduleJobRegistered scheduleJobRegistered = buildScheduleJobRegister();
        ScheduleChange registerScheduleChange = ScheduleChange.schedule_job_registered(scheduleJobRegistered);

        Event registerEvent = new Event(1L, Instant.now().toString(), Value.bin(Geck.toMsgPack(registerScheduleChange)));

        return new SignalArgs()
                .setSignal(Signal.timeout(new TimeoutSignal()))
                .setMachine(
                        new Machine()
                                .setId("schedule_id_test")
                                .setNs("schedulator")
                                .setHistory(List.of(registerEvent))
                                .setHistoryRange(new HistoryRange())
                );
    }

    private SignalArgs buildSignalTimeoutValidated() {
        Schedule businessSchedule = buildBusinessSchedule();

        ScheduleContextValidated scheduleContextValidated = new ScheduleContextValidated();
        ContextValidationResponse contextValidationResponse = new ContextValidationResponse();
        ValidationResponseStatus validationResponseStatus = new ValidationResponseStatus();
        validationResponseStatus.setSuccess(new ValidationSuccess());
        contextValidationResponse.setResponseStatus(validationResponseStatus);
        scheduleContextValidated.setResponse(contextValidationResponse);
        scheduleContextValidated.setRequest(new byte[0]);

        ScheduleChange ctxValidatedScheduleChange = ScheduleChange.schedule_context_validated(scheduleContextValidated);

        Event ctxValidatedEvent = new Event(1L, Instant.now().toString(), Value.bin(Geck.toMsgPack(ctxValidatedScheduleChange)));

        return new SignalArgs()
                .setSignal(Signal.timeout(new TimeoutSignal()))
                .setMachine(
                        new Machine()
                                .setId("schedule_id_test")
                                .setNs("schedulator")
                                .setHistory(List.of(ctxValidatedEvent))
                                .setHistoryRange(new HistoryRange())
                );
    }

    private SignalArgs buildSignalTimeoutDeregister() {
        Schedule businessSchedule = buildBusinessSchedule();

        ScheduleChange scheduleJobRegister = ScheduleChange.schedule_job_registered(buildScheduleJobRegister());

        Event registerEvent = new Event(1L, Instant.now().toString(), Value.bin(Geck.toMsgPack(scheduleJobRegister)));

        ScheduleChange scheduleChangeDeregister = ScheduleChange.schedule_job_deregistered(new ScheduleJobDeregistered());

        Event deregisterEvent = new Event(1L, Instant.now().toString(), Value.bin(Geck.toMsgPack(scheduleChangeDeregister)));

        return new SignalArgs()
                .setSignal(Signal.timeout(new TimeoutSignal()))
                .setMachine(
                        new Machine()
                            .setId("schedule_id_test")
                            .setNs("schedulator")
                            .setHistory(List.of(registerEvent, deregisterEvent))
                            .setHistoryRange(new HistoryRange())
                );
    }

    private SignalArgs buildSignalInit() {
        ScheduleJobRegistered scheduleJobRegistered = buildScheduleJobRegister();

        ScheduleChange scheduleChange = ScheduleChange.schedule_job_registered(scheduleJobRegistered);

        return new SignalArgs()
                .setSignal(Signal.init(new InitSignal(Value.bin(Geck.toMsgPack(scheduleChange)))))
                .setMachine(
                        new Machine()
                            .setId("schedule_id_test")
                            .setNs("schedulator")
                            .setHistory(new ArrayList<>())
                            .setHistoryRange(new HistoryRange())
                );
    }

    private ScheduleJobRegistered buildScheduleJobRegister() {
        Schedule buildBusinessSchedule = buildBusinessSchedule();

        return new ScheduleJobRegistered()
                .setScheduleId("testScheduleId")
                .setSchedule(buildBusinessSchedule)
                .setContext(new byte[0])
                .setExecutorServicePath("executorServicePathTest");
    }

    private Schedule buildBusinessSchedule() {
        BusinessScheduleRef businessScheduleRef = new BusinessScheduleRef();
        businessScheduleRef.setId(64);

        DominantBasedSchedule dominantBasedSchedule = new DominantBasedSchedule();
        dominantBasedSchedule.setBusinessScheduleRef(businessScheduleRef);
        dominantBasedSchedule.setCalendarRef(new CalendarRef().setId(64));
        dominantBasedSchedule.setRevision(432542L);

        Schedule schedule = new Schedule();
        schedule.setDominantSchedule(dominantBasedSchedule);

        return schedule;
    }

}
