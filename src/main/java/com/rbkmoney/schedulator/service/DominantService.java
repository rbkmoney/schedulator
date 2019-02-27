package com.rbkmoney.schedulator.service;

import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.schedulator.exception.NotFoundException;

public interface DominantService {
    BusinessSchedule getBusinessSchedule(BusinessScheduleRef scheduleRef, long domainRevision) throws NotFoundException;
    Calendar getCalendar(CalendarRef calendarRef, long domainRevision) throws NotFoundException;
}
