package com.rbkmoney.schedulator;

import com.rbkmoney.damsel.domain.BusinessSchedule;
import com.rbkmoney.damsel.domain.BusinessScheduleRef;
import com.rbkmoney.damsel.domain.Calendar;
import com.rbkmoney.damsel.domain.CalendarRef;
import com.rbkmoney.damsel.schedule.*;
import com.rbkmoney.schedulator.service.DominantService;
import com.rbkmoney.schedulator.service.RemoteClientManager;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.mockito.Mockito.*;

public class SchedulatorIntegrationTest extends AbstractIntegrationTest {

    @MockBean
    private RemoteClientManager remoteClientManager;

    @MockBean
    private DominantService dominantService;

    @Autowired
    private SchedulatorSrv.Iface schedulatorService;

    @Test
    public void registerAndDeregisterJobTest() throws TException, IOException, InterruptedException {
        ValidationResponseStatus successValidationStatus = new ValidationResponseStatus();
        successValidationStatus.setSuccess(new ValidationSuccess());
        ValidationResponseStatus failValidationStatus = new ValidationResponseStatus();
        failValidationStatus.setFailed(new ValidationFailed());
        when(remoteClientManager.validateExecutionContext(anyString(), any(ByteBuffer.class)))
                .thenReturn(new ContextValidationResponse(successValidationStatus));
        BusinessSchedule businessSchedule = ScheduleTestData.buildSchedule(2020, null, null, null, null, null, null);
        when(dominantService.getBusinessSchedule(any(BusinessScheduleRef.class), anyLong())).thenReturn(businessSchedule);
        Calendar calendar = ScheduleTestData.buildTestCalendar();
        when(dominantService.getCalendar(any(CalendarRef.class), anyLong())).thenReturn(calendar);

        RegisterJobRequest registerJobRequest = ScheduleTestData.buildRegisterJobRequest();

        schedulatorService.registerJob("testJob", registerJobRequest);
        schedulatorService.deregisterJob("testJob");
    }

}
