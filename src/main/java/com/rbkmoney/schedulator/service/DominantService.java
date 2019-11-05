package com.rbkmoney.schedulator.service;

import com.rbkmoney.damsel.domain.BusinessSchedule;
import com.rbkmoney.damsel.domain.BusinessScheduleRef;
import com.rbkmoney.damsel.domain.Calendar;
import com.rbkmoney.damsel.domain.CalendarRef;
import com.rbkmoney.schedulator.exception.NotFoundException;

public interface DominantService {
    BusinessSchedule getBusinessSchedule(BusinessScheduleRef scheduleRef, long domainRevision) throws NotFoundException;

    Calendar getCalendar(CalendarRef calendarRef, long domainRevision) throws NotFoundException;
}
