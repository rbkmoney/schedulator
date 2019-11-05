package com.rbkmoney.schedulator.cron;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.time.ZonedDateTime;

@AllArgsConstructor
@Builder
@ToString
@Getter
public class SchedulerComputeResult {

    private final ZonedDateTime prevFireTime;

    private final ZonedDateTime nextFireTime;

    private final ZonedDateTime nextCronFireTime;

}
