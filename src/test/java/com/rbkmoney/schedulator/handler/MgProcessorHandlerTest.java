package com.rbkmoney.schedulator.handler;

import com.rbkmoney.damsel.base.DayOfWeek;
import com.rbkmoney.damsel.base.Month;
import com.rbkmoney.damsel.domain.BusinessSchedule;
import com.rbkmoney.damsel.domain.BusinessScheduleRef;
import com.rbkmoney.damsel.domain.Calendar;
import com.rbkmoney.damsel.domain.CalendarRef;
import com.rbkmoney.damsel.schedule.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.geck.serializer.Geck;
import com.rbkmoney.machinegun.base.Timer;
import com.rbkmoney.machinegun.msgpack.Value;
import com.rbkmoney.machinegun.stateproc.*;
import com.rbkmoney.schedulator.ScheduleTestData;
import com.rbkmoney.schedulator.exception.NotFoundException;
import com.rbkmoney.schedulator.serializer.MachineStateSerializer;
import com.rbkmoney.schedulator.serializer.SchedulatorMachineState;
import com.rbkmoney.schedulator.service.DominantService;
import com.rbkmoney.schedulator.service.RemoteClientManager;
import com.rbkmoney.schedulator.service.ScheduleJobService;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.remoting.RemoteAccessException;
import org.springframework.test.context.junit4.SpringRunner;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MgProcessorHandlerTest {

    private static final String NEXT_FIRE_TIME = TypeUtil.temporalToString(Instant.now());

    @MockBean
    private DominantService dominantServiceMock;

    @MockBean
    private RemoteClientManager remoteClientManagerMock;

    @SpyBean
    private ScheduleJobService scheduleJobService;

    @Autowired
    private MachineStateSerializer machineStateSerializer;

    @Autowired
    private ProcessorSrv.Iface mgProcessorHandler;

    @Before
    public void setUp() throws Exception {
        ScheduledJobContext scheduledJobContext = new ScheduledJobContext()
                .setNextCronTime("testCron")
                .setNextFireTime(NEXT_FIRE_TIME)
                .setPrevFireTime("215426536");
        doReturn(scheduledJobContext).when(scheduleJobService).calculateScheduledJobContext(any(ScheduleJobRegistered.class));

        ScheduledJobExecutorSrv.Iface jobExecutorMock = mock(ScheduledJobExecutorSrv.Iface.class);
        ContextValidationResponse validationResponse = new ContextValidationResponse();
        ValidationResponseStatus validationResponseStatus = new ValidationResponseStatus();
        validationResponseStatus.setSuccess(new ValidationSuccess());
        validationResponse.setResponseStatus(validationResponseStatus);
        when(jobExecutorMock.validateExecutionContext(any(ByteBuffer.class))).thenReturn(validationResponse);
        when(jobExecutorMock.executeJob(any(ExecuteJobRequest.class))).thenReturn(ByteBuffer.wrap(new byte[0]));
        when(remoteClientManagerMock.getRemoteClient(anyString())).thenReturn(jobExecutorMock);
        when(remoteClientManagerMock.validateExecutionContext(anyString(), any(ByteBuffer.class))).thenReturn(validationResponse);

        BusinessSchedule schedule = ScheduleTestData.buildSchedule(2018, Month.Apr, (byte) 4, DayOfWeek.Fri, (byte) 7, null, null);

        when(dominantServiceMock.getBusinessSchedule(any(BusinessScheduleRef.class), anyLong())).thenReturn(schedule);

        Calendar calendar = ScheduleTestData.buildTestCalendar();

        when(dominantServiceMock.getCalendar(any(CalendarRef.class), anyLong())).thenReturn(calendar);
    }

    @Test
    public void processSignalInitTest() throws TException {
        SignalArgs signalInit = buildSignalInit();
        SignalResult signalResult = mgProcessorHandler.processSignal(signalInit);

        Assert.assertEquals("Machine events should be equal to '2'", 2, signalResult.getChange().getEvents().size());
        Assert.assertTrue("Machine action should be 'timerAction'", signalResult.getAction().isSetTimer());
    }

    @Test
    public void processSignalTimeoutRegisterTest() throws TException {
        SignalArgs signalTimeoutRegister = buildSignalTimeoutRegister();
        SignalResult signalResult = mgProcessorHandler.processSignal(signalTimeoutRegister);

        Assert.assertEquals("Machine events should be equal to '1'", 1, signalResult.getChange().getEvents().size());
        Assert.assertTrue("Machine action should be 'timerAction'", signalResult.getAction().isSetTimer());
        Assert.assertEquals("Range limit should be '1'", 1, signalResult.getAction().getTimer().getSetTimer().getRange().getLimit());
    }

    @Test
    public void processSignalTimeoutDeregisterTest() throws TException {
        SignalArgs signalTimeout = buildSignalTimeoutDeregister();
        SignalResult signalResult = mgProcessorHandler.processSignal(signalTimeout);

        Assert.assertEquals("Machine events should be equal to '1'", 1, signalResult.getChange().getEvents().size());
        Assert.assertTrue("Machine action should be 'removeAction'", signalResult.getAction().isSetRemove());
    }

    @Test
    public void fixedRetryRegisterMachineTest() throws TException {
        when(remoteClientManagerMock.getRemoteClient(anyString())).thenThrow(RemoteAccessException.class);

        SignalArgs firstSignalTimeoutRegister = buildSignalTimeoutRegister();
        SignalResult firstSignalResult = mgProcessorHandler.processSignal(firstSignalTimeoutRegister);

        Assert.assertTrue("Machine action should be 'timerAction'", firstSignalResult.getAction().isSetTimer());
        Timer firstSignalResultTimer = firstSignalResult.getAction().getTimer().getSetTimer().getTimer();

        SignalArgs secondSignalTimeoutRegister = buildSignalTimeoutRegister();
        secondSignalTimeoutRegister.getMachine().setTimer(firstSignalResultTimer.getDeadline());

        SignalResult secondSignalResult = mgProcessorHandler.processSignal(secondSignalTimeoutRegister);
        Assert.assertTrue("Machine action should be 'timerAction'", firstSignalResult.getAction().isSetTimer());
        Timer secondSignalResultTimer = secondSignalResult.getAction().getTimer().getSetTimer().getTimer();

        SignalArgs thirdSignalTimeoutRegister = buildSignalTimeoutRegister();
        thirdSignalTimeoutRegister.getMachine().setTimer(secondSignalResultTimer.getDeadline());

        SignalResult thirdSignalResult = mgProcessorHandler.processSignal(thirdSignalTimeoutRegister);
        Assert.assertTrue("Machine action should be 'timerAction'", thirdSignalResult.getAction().isSetTimer());
        Timer thirdSignalResultTimer = thirdSignalResult.getAction().getTimer().getSetTimer().getTimer();

        Instant firstSignalInstant = TypeUtil.stringToInstant(firstSignalResultTimer.getDeadline());
        Instant secondSignalInstant = TypeUtil.stringToInstant(secondSignalResultTimer.getDeadline());
        Instant thirdSignalInstant = TypeUtil.stringToInstant(thirdSignalResultTimer.getDeadline());

        long firstDuration = Duration.between(firstSignalInstant, secondSignalInstant).getSeconds();
        long secondDuration = Duration.between(secondSignalInstant, thirdSignalInstant).getSeconds();

        Assert.assertEquals("Duration between signal should be equals", firstDuration, secondDuration);
    }

    @Test
    public void fixedRetryExecuteMachineTest() throws TException {
        SignalArgs signalTimeoutRegister = buildSignalTimeoutRegister();
        SignalResult signalTimeoutRegisterResult = mgProcessorHandler.processSignal(signalTimeoutRegister);

        when(remoteClientManagerMock.getRemoteClient(anyString())).thenThrow(RemoteAccessException.class);

        Content content = signalTimeoutRegisterResult.getChange().getEvents().get(0);
        ScheduleChange scheduleChange = Geck.msgPackToTBase(content.getData().getBin(), ScheduleChange.class);
        SignalArgs signalTimeoutExecutedFirst = buildSignalTimeoutExecuted(signalTimeoutRegisterResult.getChange().getAuxState(), scheduleChange);
        SignalResult signalTimeoutExecutedResult = mgProcessorHandler.processSignal(signalTimeoutExecutedFirst);
        Assert.assertTrue("Machine action should be 'timerAction'", signalTimeoutExecutedResult.getAction().isSetTimer());

        SignalArgs signalTimeoutExecutedSecond = buildSignalTimeoutExecuted(signalTimeoutExecutedResult.getChange().getAuxState(), scheduleChange);
        SignalResult signalTimeoutExecutedResultSecond = mgProcessorHandler.processSignal(signalTimeoutExecutedSecond);
        Assert.assertTrue("Machine action should be 'timerAction'", signalTimeoutExecutedResultSecond.getAction().isSetTimer());

        SignalArgs signalTimeoutExecutedThird = buildSignalTimeoutExecuted(signalTimeoutExecutedResultSecond.getChange().getAuxState(), scheduleChange);
        SignalResult signalTimeoutExecutedResultThird = mgProcessorHandler.processSignal(signalTimeoutExecutedThird);
        Assert.assertTrue("Machine action should be 'timerAction'", signalTimeoutExecutedResultThird.getAction().isSetTimer());

        Timer firstTimer = signalTimeoutExecutedResult.getAction().getTimer().getSetTimer().getTimer();
        Timer secondTimer = signalTimeoutExecutedResultSecond.getAction().getTimer().getSetTimer().getTimer();
        Timer thirdTimer = signalTimeoutExecutedResultThird.getAction().getTimer().getSetTimer().getTimer();

        Instant firstSignalInstant = TypeUtil.stringToInstant(firstTimer.getDeadline());
        Instant secondSignalInstant = TypeUtil.stringToInstant(secondTimer.getDeadline());
        Instant thirdSignalInstant = TypeUtil.stringToInstant(thirdTimer.getDeadline());

        long firstDuration = Duration.between(firstSignalInstant, secondSignalInstant).getSeconds();
        long secondDuration = Duration.between(secondSignalInstant, thirdSignalInstant).getSeconds();

        Assert.assertEquals("Duration between signal should be equals", firstDuration, secondDuration);
    }

    @Test
    public void revertToNormalScheduleTest() throws TException {
        ScheduledJobExecutorSrv.Iface jobExecutorMock = mock(ScheduledJobExecutorSrv.Iface.class);
        ContextValidationResponse validationResponse = new ContextValidationResponse();
        ValidationResponseStatus validationResponseStatus = new ValidationResponseStatus();
        validationResponseStatus.setSuccess(new ValidationSuccess());
        validationResponse.setResponseStatus(validationResponseStatus);
        when(jobExecutorMock.validateExecutionContext(any(ByteBuffer.class))).thenReturn(validationResponse);
        when(jobExecutorMock.executeJob(any(ExecuteJobRequest.class))).thenReturn(ByteBuffer.wrap(new byte[0]));

        when(remoteClientManagerMock.getRemoteClient(anyString()))
                .thenThrow(RemoteAccessException.class)
                .thenReturn(jobExecutorMock);

        SignalArgs firstSignalTimeoutRegister = buildSignalTimeoutRegister();
        SignalResult firstSignalResult = mgProcessorHandler.processSignal(firstSignalTimeoutRegister);

        Assert.assertTrue("Machine action should be 'timerAction'", firstSignalResult.getAction().isSetTimer());

        SignalArgs secondSignalTimeoutRegister = buildSignalTimeoutRegister();
        SignalResult secondSignalResult = mgProcessorHandler.processSignal(secondSignalTimeoutRegister);

        Assert.assertTrue("Machine action should be 'timerAction'", firstSignalResult.getAction().isSetTimer());

        String deadline = secondSignalResult.getAction().getTimer().getSetTimer().getTimer().getDeadline();
        Assert.assertEquals("NextFire time should't be from retry calculator", NEXT_FIRE_TIME, deadline);
    }

    @Test
    public void eventSerializeTest() throws TException {
        SignalArgs signalTimeoutRegister = buildSignalTimeoutRegister();

        Event registerEvent = signalTimeoutRegister.getMachine().getHistory().get(0);
        ScheduleChange scheduleChange = Geck.msgPackToTBase(registerEvent.getData().getBin(), ScheduleChange.class);
        SchedulatorMachineState jobRegistered = new SchedulatorMachineState(scheduleChange.getScheduleJobRegistered());

        byte[] state = machineStateSerializer.serialize(jobRegistered);
        signalTimeoutRegister.getMachine().setAuxState(new Content(Value.bin(state)));
        SignalResult signalResult = mgProcessorHandler.processSignal(signalTimeoutRegister);

        SchedulatorMachineState machineState = machineStateSerializer.deserializer(signalResult.getChange().getAuxState().getData().getBin());

        Assert.assertEquals(scheduleChange.getScheduleJobRegistered().getExecutorServicePath(),
                machineState.getRegisterState().getExecutorServicePath());
        Assert.assertEquals(scheduleChange.getScheduleJobRegistered().getSchedule().getDominantSchedule().getRevision(),
                (long) machineState.getRegisterState().getDominantRevisionId());
        Assert.assertEquals(scheduleChange.getScheduleJobRegistered().getSchedule().getDominantSchedule().getCalendarRef().getId(),
                (long) machineState.getRegisterState().getCalendarId());
        Assert.assertEquals(scheduleChange.getScheduleJobRegistered().getSchedule().getDominantSchedule().getBusinessScheduleRef().getId(),
                (int) machineState.getRegisterState().getBusinessSchedulerId());
        Assert.assertEquals(new String(scheduleChange.getScheduleJobRegistered().getContext()),
                new String(machineState.getRegisterState().getContext().getBytes()));
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

    private SignalArgs buildSignalTimeoutDeregister() {
        ScheduleChange scheduleChangeDeregister = ScheduleChange.schedule_job_deregistered(new ScheduleJobDeregistered());

        Event deregisterEvent = new Event(1L, Instant.now().toString(), Value.bin(Geck.toMsgPack(scheduleChangeDeregister)));

        return new SignalArgs()
                .setSignal(Signal.timeout(new TimeoutSignal()))
                .setMachine(
                        new Machine()
                                .setId("schedule_id_test")
                                .setNs("schedulator")
                                .setHistory(List.of(deregisterEvent))
                                .setHistoryRange(new HistoryRange())
                );
    }

    private SignalArgs buildSignalTimeoutExecuted(Content auxState, ScheduleChange scheduleChange) {
        ExecuteJobRequest executeJobRequest = new ExecuteJobRequest();
        ScheduleChange scheduleJobExecuted = ScheduleChange.schedule_job_executed(new ScheduleJobExecuted());
        Event deregisterEvent = new Event(1L, Instant.now().toString(), Value.bin(Geck.toMsgPack(scheduleChange)));

        return new SignalArgs()
                .setSignal(Signal.timeout(new TimeoutSignal()))
                .setMachine(
                        new Machine()
                                .setId("schedule_id_test")
                                .setNs("schedulator")
                                .setHistory(List.of(deregisterEvent))
                                .setHistoryRange(new HistoryRange())
                                .setAuxState(auxState)
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
                .setContext("testContext".getBytes())
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
