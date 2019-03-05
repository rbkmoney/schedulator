package com.rbkmoney.schedulator.service.impl;

import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.domain_config.Reference;
import com.rbkmoney.damsel.domain_config.*;
import com.rbkmoney.schedulator.exception.NotFoundException;
import com.rbkmoney.schedulator.service.DominantService;
import com.rbkmoney.woody.api.flow.error.WUndefinedResultException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class DominantServiceImpl implements DominantService {

    private final RepositoryClientSrv.Iface dominantClient;
    private final RetryTemplate retryTemplate;

    @Override
    public BusinessSchedule getBusinessSchedule(BusinessScheduleRef scheduleRef, long domainRevision) throws NotFoundException {
        Reference revisionReference = Reference.version(domainRevision);
        log.info("Trying to get schedule, scheduleRef='{}', revisionReference='{}'", scheduleRef, revisionReference);
        try {
            com.rbkmoney.damsel.domain.Reference reference = new com.rbkmoney.damsel.domain.Reference();
            reference.setBusinessSchedule(scheduleRef);
            VersionedObject versionedObject = checkoutObject(revisionReference, reference);
            BusinessSchedule schedule = versionedObject.getObject().getBusinessSchedule().getData();
            log.info("Schedule has been found, scheduleRef='{}', revisionReference='{}', schedule='{}'", scheduleRef, revisionReference, schedule);
            return schedule;
        } catch (VersionNotFound | ObjectNotFound ex) {
            throw new WUndefinedResultException(String.format("Version not found, scheduleRef='%s', revisionReference='%s'", scheduleRef, revisionReference), ex);
        } catch (TException ex) {
            throw new RuntimeException(String.format("Failed to get schedule, scheduleRef='%s', revisionReference='%s'", scheduleRef, revisionReference), ex);
        }
    }



    @Override
    public Calendar getCalendar(CalendarRef calendarRef, long domainRevision) throws NotFoundException {
        Reference revisionReference = Reference.version(domainRevision);
        log.info("Trying to get calendar, calendarRef='{}', revisionReference='{}'", calendarRef, revisionReference);
        try {
            com.rbkmoney.damsel.domain.Reference reference = new com.rbkmoney.damsel.domain.Reference();
            reference.setCalendar(calendarRef);
            VersionedObject versionedObject = checkoutObject(revisionReference, reference);
            Calendar calendar = versionedObject.getObject().getCalendar().getData();
            log.info("Calendar has been found, calendarRef='{}', revisionReference='{}', calendar='{}'", calendarRef, revisionReference, calendar);
            return calendar;
        } catch (VersionNotFound | ObjectNotFound ex) {
            throw new WUndefinedResultException(String.format("Version not found, calendarRef='%s', revisionReference='%s'", calendarRef, revisionReference), ex);
        } catch (TException ex) {
            throw new RuntimeException(String.format("Failed to get calendar, calendarRef='%s', revisionReference='%s'", calendarRef, revisionReference), ex);
        }
    }

    private VersionedObject checkoutObject(Reference revisionReference, com.rbkmoney.damsel.domain.Reference reference) throws TException {
        return retryTemplate.execute(
                context -> dominantClient.checkoutObject(revisionReference, reference)
        );
    }
}
