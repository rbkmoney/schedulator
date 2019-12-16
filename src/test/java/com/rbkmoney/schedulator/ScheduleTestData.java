package com.rbkmoney.schedulator;

import com.opencsv.CSVReader;
import com.rbkmoney.damsel.base.*;
import com.rbkmoney.damsel.domain.BusinessSchedule;
import com.rbkmoney.damsel.domain.Calendar;
import com.rbkmoney.damsel.domain.CalendarHoliday;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ScheduleTestData {

    public static Calendar buildTestCalendar() throws IOException {
        Calendar calendar = new Calendar();
        calendar.setName("test-calendar");
        calendar.setTimezone("Europe/Moscow");

        ClassPathResource resource = new ClassPathResource("/data/calendar-test.csv");
        try (CSVReader reader = new CSVReader(new InputStreamReader(resource.getInputStream()))) {
            reader.readNext();

            String[] row;
            Map<Integer, Set<CalendarHoliday>> years = new HashMap<>();
            while ((row = reader.readNext()) != null) {
                Set<CalendarHoliday> calendarHolidays = new HashSet<>();
                for (int monthValue = 1; monthValue <= 12; monthValue++) {
                    Month month = Month.findByValue(monthValue);
                    for (String day : row[monthValue].split(",")) {
                        if (!day.endsWith("*")) {
                            CalendarHoliday holiday = new CalendarHoliday();
                            holiday.setName("holiday");
                            holiday.setDay(Byte.parseByte(day));
                            holiday.setMonth(month);
                            calendarHolidays.add(holiday);
                        }
                    }
                }
                int year = Integer.parseInt(row[0]);
                years.put(year, calendarHolidays);
            }
            calendar.setHolidays(years);
        }
        return calendar;
    }

    public static BusinessSchedule buildSchedule(Integer year,
                                                 Month month,
                                                 Byte dayOfMonth,
                                                 DayOfWeek dayOfWeek,
                                                 Byte hour,
                                                 Byte minute,
                                                 Byte second) {
        com.rbkmoney.damsel.base.Schedule schedule = new com.rbkmoney.damsel.base.Schedule();

        ScheduleYear scheduleYear = new ScheduleYear();
        if (year == null) {
            scheduleYear.setEvery(new ScheduleEvery());
        } else {
            scheduleYear.setOn(Set.of(year));
        }
        schedule.setYear(scheduleYear);

        ScheduleMonth scheduleMonth = new ScheduleMonth();
        scheduleMonth.setOn(Set.of(month));
        schedule.setMonth(scheduleMonth);

        ScheduleFragment scheduleDayOfMonth = new ScheduleFragment();
        if (dayOfMonth == null) {
            scheduleDayOfMonth.setEvery(new ScheduleEvery());
        } else {
            scheduleDayOfMonth.setOn(Set.of(dayOfMonth));
        }
        schedule.setDayOfMonth(scheduleDayOfMonth);

        ScheduleDayOfWeek scheduleDayOfWeek = new ScheduleDayOfWeek();
        if (dayOfWeek == null) {
            scheduleDayOfWeek.setEvery(new ScheduleEvery());
        } else {
            scheduleDayOfWeek.setOn(Set.of(dayOfWeek));
        }
        schedule.setDayOfWeek(scheduleDayOfWeek);

        ScheduleFragment scheduleHour = new ScheduleFragment();
        if (hour == null) {
            scheduleHour.setEvery(new ScheduleEvery());
        } else {
            scheduleHour.setOn(Set.of(hour));
        }
        schedule.setHour(scheduleHour);

        ScheduleFragment scheduleMinute = new ScheduleFragment();
        if (minute == null) {
            scheduleMinute.setEvery(new ScheduleEvery());
        } else {
            scheduleMinute.setOn(Set.of(minute));
        }
        schedule.setMinute(scheduleMinute);

        ScheduleFragment scheduleSecond = new ScheduleFragment();
        if (second == null) {
            scheduleSecond.setEvery(new ScheduleEvery());
        } else {
            scheduleSecond.setOn(Set.of(second));
        }
        schedule.setSecond(scheduleSecond);

        BusinessSchedule businessSchedule = new BusinessSchedule();
        businessSchedule.setSchedule(schedule);

        return businessSchedule;
    }

}
