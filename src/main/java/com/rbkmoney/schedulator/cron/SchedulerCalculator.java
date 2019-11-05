package com.rbkmoney.schedulator.cron;

import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
import com.google.common.base.Preconditions;
import com.rbkmoney.damsel.domain.Calendar;
import com.rbkmoney.schedulator.util.SchedulerUtil;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
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

    public SchedulerCalculator(String cronExpression,
                               Calendar calendar,
                               SchedulerCalculatorConfig calculatorConfig) {
        Preconditions.checkNotNull(cronExpression, "cronExpression can't be null");
        Preconditions.checkNotNull(calculatorConfig.getStartTime(), "startTime can't be null");
        this.cron = SchedulerUtil.QUARTZ_CRON_PARSER.parse(cronExpression).validate();
        this.calendar = calendar;
        this.calculatorConfig = calculatorConfig;
        this.dateAdjuster = new ExcludeHolidayAdjuster(calendar);
        this.timeZone = TimeZone.getTimeZone(calendar.getTimezone());
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
        result = result.minusYears(calculatorConfig.getYears());
        result = result.minusMonths(calculatorConfig.getMonths());
        result = result.with(dateAdjuster.adjust(
                calculatorConfig.getDays() * -1,
                calculatorConfig.getHours() * -1,
                calculatorConfig.getMinutes() * -1,
                calculatorConfig.getSeconds() * -1)
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
            nextFireTime = nextFireTime.plusYears(calculatorConfig.getYears());
            nextFireTime = nextFireTime.plusMonths(calculatorConfig.getMonths());
            nextFireTime = nextFireTime.with(dateAdjuster.adjust(
                    calculatorConfig.getDays(),
                    calculatorConfig.getHours(),
                    calculatorConfig.getMinutes(),
                    calculatorConfig.getSeconds()
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
                    throw new IllegalStateException(String.format("Can't get lastExecution for cron '%s' with '%s'", cron, date));
                });
        return nextCronTime.toLocalDateTime();
    }

    private LocalDateTime findNextCronExecution(LocalDateTime date) {
        ZonedDateTime nextCronTime = ExecutionTime.forCron(cron)
                .nextExecution(date.atZone(timeZone.toZoneId()))
                .orElseThrow(() -> {
                    throw new IllegalStateException(String.format("Can't get nextExecution for cron '%s' with '%s'", cron, date));
                });

        return nextCronTime.toLocalDateTime();
    }

    private boolean isDateShift() {
        return calculatorConfig.getYears() > 0 ||
                calculatorConfig.getDays() > 0 ||
                calculatorConfig.getMonths() > 0 ||
                calculatorConfig.getMinutes() > 0 ||
                calculatorConfig.getHours() > 0 ||
                calculatorConfig.getSeconds() > 0;
    }

    @Data
    @Builder
    private static final class NextTime {

        private LocalDateTime prevFireTime;

        private LocalDateTime nextCronTime;

        private LocalDateTime nextFireTime;

    }

}
