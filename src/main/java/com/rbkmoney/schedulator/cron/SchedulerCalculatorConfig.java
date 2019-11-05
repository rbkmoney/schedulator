package com.rbkmoney.schedulator.cron;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class SchedulerCalculatorConfig {

    LocalDateTime startTime;

    private int years;

    private int months;

    private int days;

    private int hours;

    private int minutes;

    private int seconds;

}
