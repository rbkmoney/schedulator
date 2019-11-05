package com.rbkmoney.schedulator.cron;

import java.time.temporal.TemporalAdjuster;

public interface DateAdjuster {

    public TemporalAdjuster adjust(long days, long hour, long minutes, long seconds);

}
