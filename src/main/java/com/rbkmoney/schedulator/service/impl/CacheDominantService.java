package com.rbkmoney.schedulator.service.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.rbkmoney.damsel.domain.BusinessSchedule;
import com.rbkmoney.damsel.domain.BusinessScheduleRef;
import com.rbkmoney.damsel.domain.Calendar;
import com.rbkmoney.damsel.domain.CalendarRef;
import com.rbkmoney.schedulator.exception.NotFoundException;
import com.rbkmoney.schedulator.service.DominantService;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Component
@Primary
@RequiredArgsConstructor
public class CacheDominantService implements DominantService {

    private final Cache<Long, Calendar> dominantCache;

    private final DominantServiceImpl dominantService;

    @Override
    public BusinessSchedule getBusinessSchedule(BusinessScheduleRef scheduleRef, Long domainRevision) throws NotFoundException {
        return dominantService.getBusinessSchedule(scheduleRef, domainRevision);
    }

    @Override
    public Calendar getCalendar(CalendarRef calendarRef, Long domainRevision) throws NotFoundException {
        long key = calendarRef.getId() + domainRevision;
        return dominantCache.get(key, s -> dominantService.getCalendar(calendarRef, domainRevision));
    }
}
