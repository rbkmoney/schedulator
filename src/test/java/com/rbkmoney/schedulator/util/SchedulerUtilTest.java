package com.rbkmoney.schedulator.util;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.opencsv.CSVReader;
import com.rbkmoney.damsel.base.*;
import com.rbkmoney.damsel.domain.BusinessSchedule;
import com.rbkmoney.damsel.domain.Calendar;
import com.rbkmoney.damsel.domain.CalendarHoliday;
import com.rbkmoney.schedulator.ScheduleTestData;
import com.rbkmoney.schedulator.cron.DateAdjuster;
import com.rbkmoney.schedulator.cron.ExcludeHolidayAdjuster;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

import static com.rbkmoney.damsel.base.DayOfWeek.*;
import static com.rbkmoney.damsel.base.Month.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
public class SchedulerUtilTest {

    private static Calendar calendar;

    private static DateAdjuster excludeHolidayAdjuster;

    @BeforeClass
    public static void setUp() throws Exception {
        calendar = ScheduleTestData.buildTestCalendar();
        excludeHolidayAdjuster = new ExcludeHolidayAdjuster(calendar, 2018);
    }

    @Test
    public void findNearestCronTest() {
        List<String> cronExpressionList = List.of("0 0 12 ? * TUE *", "0 0 12 ? * MON *", "0 0 12 ? * WED *", "0 0 12 ? * THU *");
        ZonedDateTime dateTime = ZonedDateTime.of(2019, 11, 10, 9, 0, 0, 0, ZoneId.of(calendar.getTimezone()));
        String nearestCron = SchedulerUtil.getNearestCron(cronExpressionList, dateTime);
        Assert.assertEquals("0 0 12 ? * MON *", nearestCron);
    }

    @Test
    public void testCronBuilderScheduleEvery() {
        ScheduleEvery scheduleEvery = new ScheduleEvery();

        Schedule schedule = new Schedule(
                ScheduleYear.every(scheduleEvery),
                ScheduleMonth.every(scheduleEvery),
                ScheduleFragment.every(scheduleEvery),
                ScheduleDayOfWeek.every(scheduleEvery),
                ScheduleFragment.every(scheduleEvery),
                ScheduleFragment.every(scheduleEvery),
                ScheduleFragment.every(scheduleEvery)
        );

        List<String> cronList = SchedulerUtil.buildCron(schedule, Optional.empty());
        assertEquals(1, cronList.size());
        assertEquals("* * * * * ? *", cronList.get(0));
        assertTrue(cronList.stream().allMatch(this::isValidExpression));
    }

    @Test
    public void testCronWhenOnlyWeekSet() {
        ScheduleEvery scheduleEvery = new ScheduleEvery();
        Schedule schedule = new Schedule(
                ScheduleYear.every(scheduleEvery),
                ScheduleMonth.every(scheduleEvery),
                ScheduleFragment.every(scheduleEvery),
                ScheduleDayOfWeek.on(new HashSet<>(Arrays.asList(Mon, Sun, Sat))),
                ScheduleFragment.every(scheduleEvery),
                ScheduleFragment.every(scheduleEvery),
                ScheduleFragment.every(scheduleEvery)
        );

        List<String> cronList = SchedulerUtil.buildCron(schedule, Optional.empty());
        Cron cron = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ)).parse(cronList.get(0));
        Optional<ZonedDateTime> lastExecution = ExecutionTime.forCron(cron).lastExecution(ZonedDateTime.now());
        Optional<ZonedDateTime> nextExecution = ExecutionTime.forCron(cron).nextExecution(ZonedDateTime.now());
        assertEquals(1, cronList.size());
        assertEquals("* * * ? * 1,2,7 *", cronList.get(0));
        assertTrue(isValidExpression(cronList.get(0)));


        ScheduleEvery scheduleEvery3daysOfWeek = new ScheduleEvery();
        scheduleEvery3daysOfWeek.setNth((byte) 3);
        schedule = new Schedule(
                ScheduleYear.every(scheduleEvery),
                ScheduleMonth.every(scheduleEvery),
                ScheduleFragment.every(scheduleEvery),
                ScheduleDayOfWeek.every(scheduleEvery3daysOfWeek),
                ScheduleFragment.every(scheduleEvery),
                ScheduleFragment.every(scheduleEvery),
                ScheduleFragment.every(scheduleEvery)
        );

        cronList = SchedulerUtil.buildCron(schedule, Optional.empty());
        assertEquals(1, cronList.size());
        assertEquals("* * * ? * */3 *", cronList.get(0));
        assertTrue(isValidExpression(cronList.get(0)));
    }

    @Test
    public void testCronWhenOnlyDayOfMonthSet() {
        ScheduleEvery scheduleEvery = new ScheduleEvery();
        Schedule schedule = new Schedule(
                ScheduleYear.every(scheduleEvery),
                ScheduleMonth.every(scheduleEvery),
                ScheduleFragment.on(new HashSet<>(Arrays.asList((byte) 6, (byte) 10, (byte) 31))),
                ScheduleDayOfWeek.every(scheduleEvery),
                ScheduleFragment.every(scheduleEvery),
                ScheduleFragment.every(scheduleEvery),
                ScheduleFragment.every(scheduleEvery)
        );

        List<String> cronList = SchedulerUtil.buildCron(schedule, Optional.empty());
        assertEquals(1, cronList.size());
        assertEquals("* * * 6,10,31 * ? *", cronList.get(0));
        assertTrue(isValidExpression(cronList.get(0)));


        ScheduleEvery scheduleEvery3daysOfMonth = new ScheduleEvery();
        scheduleEvery3daysOfMonth.setNth((byte) 3);
        schedule = new Schedule(
                ScheduleYear.every(scheduleEvery),
                ScheduleMonth.every(scheduleEvery),
                ScheduleFragment.every(scheduleEvery3daysOfMonth),
                ScheduleDayOfWeek.every(scheduleEvery),
                ScheduleFragment.every(scheduleEvery),
                ScheduleFragment.every(scheduleEvery),
                ScheduleFragment.every(scheduleEvery)
        );

        cronList = SchedulerUtil.buildCron(schedule, Optional.empty());
        assertEquals(1, cronList.size());
        assertEquals("* * * */3 * ? *", cronList.get(0));
        assertTrue(isValidExpression(cronList.get(0)));
    }

    @Test
    public void testCronBuilderWithCustomValues() {
        ScheduleEvery scheduleEvery = new ScheduleEvery();
        scheduleEvery.setNth((byte) 5);

        Schedule schedule = new Schedule(
                ScheduleYear.every(scheduleEvery),
                ScheduleMonth.on(new HashSet<>(Arrays.asList(Jan, Feb, Mar, Apr, Oct, Nov))),
                ScheduleFragment.every(scheduleEvery),
                ScheduleDayOfWeek.on(new HashSet<>(Arrays.asList(Mon, Tue, Fri))),
                ScheduleFragment.on(new HashSet<>(Arrays.asList((byte) 1, (byte) 3, (byte) 4, (byte) 5, (byte) 12))),
                ScheduleFragment.every(scheduleEvery),
                ScheduleFragment.every(scheduleEvery)
        );

        List<String> cronList = SchedulerUtil.buildCron(schedule, Optional.empty());
        assertEquals(2, cronList.size());
        assertEquals("*/5 */5 1,3,4,5,12 */5 1,2,3,4,10,11 ? */5", cronList.get(0));
        assertEquals("*/5 */5 1,3,4,5,12 ? 1,2,3,4,10,11 2,3,6 */5", cronList.get(1));
        assertTrue(cronList.stream().allMatch(this::isValidExpression));
    }

    @Test
    public void testDOWToQuartzFormat() {
        Schedule schedule = new Schedule(
                ScheduleYear.every(new ScheduleEvery()),
                ScheduleMonth.every(new ScheduleEvery()),
                ScheduleFragment.every(new ScheduleEvery()),
                ScheduleDayOfWeek.on(new HashSet<>(Arrays.asList(Mon, Fri, Sun))),
                ScheduleFragment.every(new ScheduleEvery()),
                ScheduleFragment.every(new ScheduleEvery()),
                ScheduleFragment.every(new ScheduleEvery())
        );

        List<String> cronList = SchedulerUtil.buildCron(schedule, Optional.ofNullable(com.rbkmoney.damsel.base.DayOfWeek.Tue));
        assertEquals(1, cronList.size());
        assertEquals("* * * ? * 1,5,7 *", cronList.get(0));

        cronList = SchedulerUtil.buildCron(schedule, Optional.ofNullable(com.rbkmoney.damsel.base.DayOfWeek.Mon));
        assertEquals(1, cronList.size());
        assertEquals("* * * ? * 1,2,6 *", cronList.get(0));
    }

    private boolean isValidExpression(String cronExpression) {
        CronParser parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));
        Cron quartzCron = parser.parse(cronExpression);
        try {
            quartzCron.validate();
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

}
