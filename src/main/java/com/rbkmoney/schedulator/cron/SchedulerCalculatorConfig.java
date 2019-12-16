package com.rbkmoney.schedulator.cron;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class SchedulerCalculatorConfig {

    LocalDateTime startTime;

    private int calendarYear;

    private int delayYears;

    private int delayMonths;

    private int delayDays;

    private int delayHours;

    private int delayMinutes;

    private int delaySeconds;

}
