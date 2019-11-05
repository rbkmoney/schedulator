package com.rbkmoney.schedulator.cron;

import com.rbkmoney.damsel.domain.Calendar;
import com.rbkmoney.damsel.domain.CalendarHoliday;
import lombok.RequiredArgsConstructor;

import java.time.LocalDateTime;
import java.time.temporal.TemporalAdjuster;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class ExcludeHolidayAdjuster implements DateAdjuster {

    private final Calendar calendar;

    @Override
    public TemporalAdjuster adjust(long days, long hour, long minutes, long seconds) {
        return (temporal -> {
            LocalDateTime dateTime = LocalDateTime.from(temporal);
            dateTime = addWorkingTime(dateTime, days, TimeUnit.DAYS);
            dateTime = addWorkingTime(dateTime, hour, TimeUnit.HOURS);
            dateTime = addWorkingTime(dateTime, minutes, TimeUnit.MINUTES);
            dateTime = addWorkingTime(dateTime, seconds, TimeUnit.SECONDS);

            return temporal.with(dateTime);
        });
    }

    private LocalDateTime addWorkingTime(LocalDateTime initDate, long workingTime, TimeUnit timeUnit) {
        if (workingTime == 0) {
            return isHoliday(initDate) ? nextWorkingTime(initDate, 1, timeUnit) : initDate; // next or same working day
        }

        LocalDateTime result = initDate;
        int step = Long.signum(workingTime); // forward or backward

        for (long i = 0; i < Math.abs(workingTime); i++) {
            result = nextWorkingTime(result, step, timeUnit);
        }

        return result;
    }

    private LocalDateTime nextWorkingTime(LocalDateTime date, int step, TimeUnit timeUnit) {
        do {
            switch (timeUnit) {
                case DAYS:
                    date = date.plusDays(step);
                    break;
                case HOURS:
                    date = date.plusHours(step);
                    break;
                case MINUTES:
                    date = date.plusMinutes(step);
                    break;
                case SECONDS:
                    date = date.plusSeconds(step);
                    break;
                default:
                    throw new IllegalStateException("Unknown timeUnit: " + timeUnit);
            }
        } while (isHoliday(date));

        return date;
    }

    private boolean isHoliday(LocalDateTime date) {
        Set<CalendarHoliday> calendarHolidays = calendar.getHolidays().get(date.getYear());
        if (calendarHolidays == null) {
            throw new IllegalStateException("Year '" + date.getYear() + "' not found on calendar");
        }
        return calendarHolidays.stream()
                .anyMatch(calendarHoliday -> {
                    if (calendarHoliday.getMonth().getValue() == date.getMonth().getValue()) {
                        return calendarHoliday.getDay() == date.getDayOfMonth();
                    }
                    return false;
                });
    }

}
