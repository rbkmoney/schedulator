package com.rbkmoney.schedulator.cron;

import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
import com.google.common.base.Preconditions;
import com.rbkmoney.damsel.base.ScheduleYear;
import com.rbkmoney.damsel.domain.BusinessSchedule;
import com.rbkmoney.damsel.domain.Calendar;
import com.rbkmoney.schedulator.util.SchedulerUtil;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

public class SchedulerCalculator {

    private final Cron cron;

    private final Calendar calendar;

    private final SchedulerCalculatorConfig calculatorConfig;

    private final DateAdjuster dateAdjuster;

    private final TimeZone timeZone;

    private LocalDateTime lastPrevFireTime;
    private LocalDateTime lastNextFireTime;
    private LocalDateTime lastNextCronTime;

    SchedulerCalculator(String cronExpression,
                        Calendar calendar,
                        SchedulerCalculatorConfig calculatorConfig) {
        Preconditions.checkNotNull(cronExpression, "cronExpression can't be null");
        Preconditions.checkNotNull(calculatorConfig.getStartTime(), "startTime can't be null");
        this.cron = SchedulerUtil.QUARTZ_CRON_PARSER.parse(cronExpression).validate();
        this.calendar = calendar;
        this.calculatorConfig = calculatorConfig;
        this.dateAdjuster = new ExcludeHolidayAdjuster(calendar, calculatorConfig.getCalendarYear());
        this.timeZone = TimeZone.getTimeZone(calendar.getTimezone());
    }

    public static SchedulerCalculator newSchedulerCalculator(ZonedDateTime startDateTime, Calendar calendar, BusinessSchedule schedule) {
        List<String> cronList = SchedulerUtil.buildCron(schedule.getSchedule(), Optional.ofNullable(calendar.getFirstDayOfWeek()));
        String cron = SchedulerUtil.getNearestCron(cronList, startDateTime);
        Integer year = getYear(schedule.getSchedule().getYear());
        if (year == null) {
            throw new IllegalStateException("Schedule year can't be null");
        }
        SchedulerCalculator schedulerCalculator = null;
        if (schedule.isSetDelay()) {
            SchedulerCalculatorConfig calculatorConfig = SchedulerCalculatorConfig.builder()
                    .calendarYear(year)
                    .startTime(startDateTime.toLocalDateTime())
                    .delayYears(schedule.getDelay().getYears())
                    .delayMonths(schedule.getDelay().getMonths())
                    .delayDays(schedule.getDelay().getDays())
                    .delayHours(schedule.getDelay().getHours())
                    .delayMinutes(schedule.getDelay().getMinutes())
                    .delaySeconds(schedule.getDelay().getSeconds())
                    .build();
            schedulerCalculator = new SchedulerCalculator(cron, calendar, calculatorConfig);
        } else {
            SchedulerCalculatorConfig calculatorConfig = SchedulerCalculatorConfig.builder()
                    .calendarYear(year)
                    .startTime(startDateTime.toLocalDateTime())
                    .build();
            schedulerCalculator = new SchedulerCalculator(cron, calendar, calculatorConfig);
        }
        return schedulerCalculator;
    }

    private static Integer getYear(ScheduleYear scheduleYear) {
        if (scheduleYear.isSetEvery()) {
            return LocalDateTime.now().getYear();
        } else if (scheduleYear.isSetOn()) {
            return scheduleYear.getOn().stream()
                    .findFirst()
                    .orElse(LocalDateTime.now().getYear());
        }
        return null;
    }

    public SchedulerComputeResult computeFireTime() {
        if (lastNextFireTime == null) {
            // Compute first fire time
            NextTime nextTime = firstFireTime();
            lastPrevFireTime = nextTime.getPrevFireTime();
            lastNextFireTime = nextTime.getNextFireTime();
            lastNextCronTime = nextTime.getNextCronTime();

            return SchedulerComputeResult.builder()
                    .prevFireTime(lastPrevFireTime.atZone(timeZone.toZoneId()))
                    .nextFireTime(nextTime.getNextFireTime().atZone(timeZone.toZoneId()))
                    .nextCronFireTime(nextTime.getNextCronTime().atZone(timeZone.toZoneId()))
                    .build();
        } else {
            // Compute next fire time
            NextTime nextTime = nextFireTime();
            lastPrevFireTime = lastNextFireTime;
            lastNextFireTime = nextTime.getNextFireTime();
            lastNextCronTime = nextTime.getNextCronTime();

            return SchedulerComputeResult.builder()
                    .prevFireTime(lastPrevFireTime.atZone(timeZone.toZoneId()))
                    .nextFireTime(nextTime.getNextFireTime().atZone(timeZone.toZoneId()))
                    .nextCronFireTime(nextTime.getNextCronTime().atZone(timeZone.toZoneId()))
                    .build();
        }
    }

    private NextTime firstFireTime() {
        LocalDateTime startTime = calculatorConfig.getStartTime();

        LocalDateTime prevFireTime = computePrevFireTime(startTime);

        NextTime nextTime = nextFireTime(prevFireTime.minus(1000, ChronoUnit.MILLIS));
        nextTime.setPrevFireTime(prevFireTime);

        return nextTime;
    }

    private NextTime nextFireTime() {
        return nextFireTime(null);
    }

    private NextTime nextFireTime(LocalDateTime dateTime) {
        NextTime nextDateTime = null;
        do {
            if (nextDateTime != null) {
                nextDateTime = computeNextFireTime(nextDateTime.getNextCronTime());
            } else {
                nextDateTime = computeNextFireTime(dateTime != null ? dateTime : lastNextCronTime);
            }
        } while (nextDateTime.getNextFireTime().equals(computeNextFireTime(nextDateTime.getNextCronTime()).getNextFireTime()));


        return nextDateTime;
    }

    private LocalDateTime computePrevFireTime(LocalDateTime dateTime) {
        LocalDateTime result = dateTime;
        result = result.minusYears(calculatorConfig.getDelayYears());
        result = result.minusMonths(calculatorConfig.getDelayMonths());
        result = result.with(dateAdjuster.adjust(
                -calculatorConfig.getDelayDays(),
                -calculatorConfig.getDelayHours(),
                -calculatorConfig.getDelayMinutes(),
                -calculatorConfig.getDelaySeconds())
        );

        return result;
    }

    private NextTime computeNextFireTime(LocalDateTime dateTime) {
        LocalDateTime nextCronTime = findNextCronExecution(dateTime);
        LocalDateTime nextFireTime = nextCronTime;

        // Exclude holiday only if there is a shift (day, month etc.)
        if (isDateShift()) {
            LocalDateTime excludedTime = nextFireTime.toLocalDate().atStartOfDay().with(dateAdjuster.adjust(0, 0, 0, 0)); // skip holiday

            nextFireTime = excludedTime.isAfter(nextFireTime) ? excludedTime : nextFireTime;
            nextFireTime = nextFireTime.plusYears(calculatorConfig.getDelayYears());
            nextFireTime = nextFireTime.plusMonths(calculatorConfig.getDelayMonths());
            nextFireTime = nextFireTime.with(dateAdjuster.adjust(
                    calculatorConfig.getDelayDays(),
                    calculatorConfig.getDelayHours(),
                    calculatorConfig.getDelayMinutes(),
                    calculatorConfig.getDelaySeconds()
            ));
        }

        return NextTime.builder()
                .nextFireTime(nextFireTime)
                .nextCronTime(nextCronTime)
                .build();
    }

    private LocalDateTime findPrevCronExecution(LocalDateTime date) {
        ZonedDateTime nextCronTime = ExecutionTime.forCron(cron)
                .lastExecution(date.atZone(timeZone.toZoneId()))
                .orElseThrow(() -> {
                    throw new IllegalStateException(String.format("Can't get lastExecution for cron '%s' with '%s'", cron.asString(), date));
                });
        return nextCronTime.toLocalDateTime();
    }

    private LocalDateTime findNextCronExecution(LocalDateTime date) {
        ZonedDateTime nextCronTime = ExecutionTime.forCron(cron)
                .nextExecution(date.atZone(timeZone.toZoneId()))
                .orElseThrow(() -> {
                    throw new IllegalStateException(String.format("Can't get nextExecution for cron '%s' with '%s'", cron.asString(), date));
                });

        return nextCronTime.toLocalDateTime();
    }

    private boolean isDateShift() {
        return calculatorConfig.getDelayYears() > 0 ||
                calculatorConfig.getDelayDays() > 0 ||
                calculatorConfig.getDelayMonths() > 0 ||
                calculatorConfig.getDelayMinutes() > 0 ||
                calculatorConfig.getDelayHours() > 0 ||
                calculatorConfig.getDelaySeconds() > 0;
    }

    @Data
    @Builder
    private static final class NextTime {

        private LocalDateTime prevFireTime;

        private LocalDateTime nextCronTime;

        private LocalDateTime nextFireTime;

    }

}
