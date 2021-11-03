package com.rbkmoney.schedulator.cron;

import com.rbkmoney.damsel.base.TimeSpan;
import com.rbkmoney.damsel.domain.BusinessSchedule;
import com.rbkmoney.damsel.domain.Calendar;
import com.rbkmoney.schedulator.ScheduleTestData;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.TimeZone;

@RunWith(SpringRunner.class)
public class SchedulerCalculatorTest {

    private static Calendar calendar;

    private static TimeZone timeZone;

    @BeforeClass
    public static void setUp() throws Exception {
        calendar = ScheduleTestData.buildTestCalendar();
        timeZone = TimeZone.getTimeZone(calendar.getTimezone());
    }

    @Test
    public void testEveryDayOnWeekends() {
        LocalDateTime startTime = LocalDateTime.of(2018, java.time.Month.JANUARY, 12, 0, 0, 0);
        SchedulerCalculatorConfig calculatorConfig = new SchedulerCalculatorConfig(startTime, 2018, 0, 0, 0, 1, 0, 0);
        SchedulerCalculator schedulerCalculator = new SchedulerCalculator("0 0 20 ? * * *", calendar, calculatorConfig);

        SchedulerComputeResult firstCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 1, 12, 21, 0,0, 0,
                timeZone.toZoneId()), firstCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 1, 12, 20, 0, 0,0,
                timeZone.toZoneId()), firstCalc.getNextCronFireTime());

        SchedulerComputeResult secCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 1, 15, 1, 0,0, 0,
                timeZone.toZoneId()), secCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 1, 14, 20, 0, 0,0,
                timeZone.toZoneId()), secCalc.getNextCronFireTime());
    }

    @Test
    public void testStartOfMonthOnThirdWorkingDayMay() {
        LocalDateTime startTime = LocalDateTime.of(2018, java.time.Month.APRIL, 24, 0, 0, 0);
        SchedulerCalculatorConfig calculatorConfig = new SchedulerCalculatorConfig(startTime, 2018, 0, 0, 2, 0, 0, 0);
        SchedulerCalculator schedulerCalculator = new SchedulerCalculator("0 0 0 1 * ? *", calendar, calculatorConfig);

        SchedulerComputeResult firstCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 5, 7, 0, 0,0, 0,
                timeZone.toZoneId()), firstCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 5, 1, 0, 0, 0,0,
                timeZone.toZoneId()), firstCalc.getNextCronFireTime());

        SchedulerComputeResult secCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 6, 5, 0, 0,0, 0,
                timeZone.toZoneId()), secCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 6, 1, 0, 0, 0,0,
                timeZone.toZoneId()), secCalc.getNextCronFireTime());

        SchedulerComputeResult thirdCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 6, 5, 0, 0,0, 0,
                timeZone.toZoneId()), secCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 6, 1, 0, 0, 0,0,
                timeZone.toZoneId()), secCalc.getNextCronFireTime());
    }

    @Test
    public void testStartOfMonthOnThirdWorkingDayYearStart() {
        LocalDateTime startTime = LocalDateTime.of(2017, java.time.Month.DECEMBER, 29, 0, 0, 0);
        SchedulerCalculatorConfig calculatorConfig = new SchedulerCalculatorConfig(startTime, 2018, 0, 0, 2, 0, 0, 0);
        var schedulerCalculator = new SchedulerCalculator("0 0 0 1 * ? *", calendar, calculatorConfig);

        SchedulerComputeResult firstCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 1, 11, 0, 0,0, 0,
                timeZone.toZoneId()), firstCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 1, 1, 0, 0,0, 0,
                timeZone.toZoneId()), firstCalc.getNextCronFireTime());

        SchedulerComputeResult secCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 2, 5, 0, 0,0, 0,
                timeZone.toZoneId()), secCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 2, 1, 0, 0,0, 0,
                timeZone.toZoneId()), secCalc.getNextCronFireTime());

        SchedulerComputeResult thirdCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 5, 0, 0,0, 0,
                timeZone.toZoneId()), thirdCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 1, 0, 0,0, 0,
                timeZone.toZoneId()), thirdCalc.getNextCronFireTime());
    }

    @Test
    public void testStartOfWeekOnThirdWorkingDayApril() {
        LocalDateTime startTime = LocalDateTime.of(2018, java.time.Month.APRIL, 24, 0, 0, 0);
        var calculatorConfig = new SchedulerCalculatorConfig(startTime, 2018, 0, 0, 2, 0, 0, 0);
        var schedulerCalculator = new SchedulerCalculator("0 0 0 ? * MON *", calendar, calculatorConfig);

        SchedulerComputeResult firstCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 4, 25, 0, 0,0, 0,
                timeZone.toZoneId()), firstCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 4, 23, 0, 0, 0,0,
                timeZone.toZoneId()), firstCalc.getNextCronFireTime());

        SchedulerComputeResult secCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 5, 7, 0, 0,0, 0,
                timeZone.toZoneId()), secCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 4, 30, 0, 0, 0,0,
                timeZone.toZoneId()), secCalc.getNextCronFireTime());

        SchedulerComputeResult thirdCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 5, 10, 0, 0,0, 0,
                timeZone.toZoneId()), thirdCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 5, 7, 0, 0, 0,0,
                timeZone.toZoneId()), thirdCalc.getNextCronFireTime());
    }

    @Test
    public void testStartOfWeekOnThirdWorkingNewYearHoliday() {
        LocalDateTime startTime = LocalDateTime.of(2017, java.time.Month.DECEMBER, 29, 0, 0, 0);
        var calculatorConfig = new SchedulerCalculatorConfig(startTime, 2018, 0, 0, 2, 0, 0, 0);
        var schedulerCalculator = new SchedulerCalculator("0 0 0 ? * MON *", calendar, calculatorConfig);

        SchedulerComputeResult firstCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 1, 11, 0, 0,0, 0,
                timeZone.toZoneId()), firstCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 1, 8, 0, 0, 0,0,
                timeZone.toZoneId()), firstCalc.getNextCronFireTime());

        SchedulerComputeResult secCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 1, 17, 0, 0,0, 0,
                timeZone.toZoneId()), secCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 1, 15, 0, 0, 0,0,
                timeZone.toZoneId()), secCalc.getNextCronFireTime());

        SchedulerComputeResult thridCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 1, 24, 0, 0,0, 0,
                timeZone.toZoneId()), thridCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 1, 22, 0, 0, 0,0,
                timeZone.toZoneId()), thridCalc.getNextCronFireTime());
    }

    @Test
    public void testEveryDayOnThirdWorkingDayNewYearHoliday() {
        LocalDateTime startTime = LocalDateTime.of(2017, java.time.Month.DECEMBER, 29, 0, 0, 0);
        var calculatorConfig = new SchedulerCalculatorConfig(startTime, 2018, 0, 0, 2, 0, 0, 0);
        var schedulerCalculator = new SchedulerCalculator("0 0 0 * * ? *", calendar, calculatorConfig);

        SchedulerComputeResult firstCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2017, 12, 29, 0, 0,0, 0,
                timeZone.toZoneId()), firstCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2017, 12, 27, 0, 0, 0,0,
                timeZone.toZoneId()), firstCalc.getNextCronFireTime());

        SchedulerComputeResult secCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 1, 9, 0, 0,0, 0,
                timeZone.toZoneId()), secCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2017, 12, 28, 0, 0,0, 0,
                timeZone.toZoneId()), secCalc.getNextCronFireTime());

        SchedulerComputeResult thirdCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 1, 10, 0, 0, 0, 0,
                timeZone.toZoneId()), thirdCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2017, 12, 29, 0, 0, 0, 0,
                timeZone.toZoneId()), thirdCalc.getNextCronFireTime());

        SchedulerComputeResult fourthCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 1, 11, 0, 0,0, 0,
                timeZone.toZoneId()), fourthCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 1, 9, 0, 0,0, 0,
                timeZone.toZoneId()), fourthCalc.getNextCronFireTime());

        SchedulerComputeResult fiveCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 1, 12, 0, 0,0, 0,
                timeZone.toZoneId()), fiveCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 1, 10, 0, 0,0, 0,
                timeZone.toZoneId()), fiveCalc.getNextCronFireTime());
    }

    @Test
    public void testEveryDayOnThirdWorkingDayMarchHoliday() {
        LocalDateTime startTime = LocalDateTime.of(2018, java.time.Month.MARCH, 6, 0, 0, 0);
        var calculatorConfig = new SchedulerCalculatorConfig(startTime, 2018, 0, 0, 2, 0, 0, 0);
        var schedulerCalculator = new SchedulerCalculator("0 0 0 * * ? *", calendar, calculatorConfig);

        SchedulerComputeResult firstCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 6, 0, 0,0, 0,
                timeZone.toZoneId()), firstCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 2, 0, 0, 0,0,
                timeZone.toZoneId()), firstCalc.getNextCronFireTime());

        SchedulerComputeResult secCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 7, 0, 0,0, 0,
                timeZone.toZoneId()), secCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 5, 0, 0, 0,0,
                timeZone.toZoneId()), secCalc.getNextCronFireTime());

        SchedulerComputeResult thirdCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 12, 0, 0,0, 0,
                timeZone.toZoneId()), thirdCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 6, 0, 0, 0,0,
                timeZone.toZoneId()), thirdCalc.getNextCronFireTime());

        SchedulerComputeResult fourthCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 13, 0, 0,0, 0,
                timeZone.toZoneId()), fourthCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 7, 0, 0, 0,0,
                timeZone.toZoneId()), fourthCalc.getNextCronFireTime());

        SchedulerComputeResult fiveCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 14, 0, 0,0, 0,
                timeZone.toZoneId()), fiveCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 12, 0, 0, 0,0,
                timeZone.toZoneId()), fiveCalc.getNextCronFireTime());
    }

    @Test
    public void testEveryDayAtEndOfDay() {
        LocalDateTime startTime = LocalDateTime.of(2018, java.time.Month.MARCH, 7, 0, 0, 0);
        var calculatorConfig = new SchedulerCalculatorConfig(startTime, 2018, 0, 0, 0, 0, 0, 0);
        var schedulerCalculator = new SchedulerCalculator("0 0 0 1/1 * ? *", calendar, calculatorConfig);

        SchedulerComputeResult firstCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 7, 0, 0,0, 0,
                timeZone.toZoneId()), firstCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 7, 0, 0, 0,0,
                timeZone.toZoneId()), firstCalc.getNextCronFireTime());

        SchedulerComputeResult secCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 8, 0, 0,0, 0,
                timeZone.toZoneId()), secCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 8, 0, 0, 0,0,
                timeZone.toZoneId()), secCalc.getNextCronFireTime());

        SchedulerComputeResult thirdCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 9, 0, 0,0, 0,
                timeZone.toZoneId()), thirdCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 9, 0, 0, 0,0,
                timeZone.toZoneId()), thirdCalc.getNextCronFireTime());
    }

    @Test
    public void testNewSchedulerCalculatorNoDelay() {
        BusinessSchedule businessSchedule = ScheduleTestData.buildSchedule(2018, null, null,
                null, (byte) 0, (byte) 0, (byte) 0);
        LocalDateTime startTime = LocalDateTime.of(2018, java.time.Month.MARCH, 7, 0, 0, 0);
        ZonedDateTime starTimeZone = ZonedDateTime.of(startTime, timeZone.toZoneId());
        var schedulerCalculator = SchedulerCalculator.newSchedulerCalculator(starTimeZone, calendar, businessSchedule);

        SchedulerComputeResult firstCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 7, 0, 0,0, 0,
                timeZone.toZoneId()), firstCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 7, 0, 0, 0,0,
                timeZone.toZoneId()), firstCalc.getNextCronFireTime());

        SchedulerComputeResult secCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 8, 0, 0,0, 0,
                timeZone.toZoneId()), secCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 8, 0, 0, 0,0,
                timeZone.toZoneId()), secCalc.getNextCronFireTime());

        SchedulerComputeResult thirdCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 9, 0, 0,0, 0,
                timeZone.toZoneId()), thirdCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 9, 0, 0, 0,0,
                timeZone.toZoneId()), thirdCalc.getNextCronFireTime());
    }

    @Test
    public void testNewSchedulerCalculatorWithDelay() {
        BusinessSchedule businessSchedule = ScheduleTestData.buildSchedule(2018, null, null,
                null, (byte) 0, (byte) 0, (byte) 0);
        TimeSpan timeSpan = new TimeSpan();
        timeSpan.setHours((short) 1);
        businessSchedule.setDelay(timeSpan);
        LocalDateTime startTime = LocalDateTime.of(2018, java.time.Month.MARCH, 7, 0, 0, 0);
        ZonedDateTime starTimeZone = ZonedDateTime.of(startTime, timeZone.toZoneId());
        var schedulerCalculator = SchedulerCalculator.newSchedulerCalculator(starTimeZone, calendar, businessSchedule);

        SchedulerComputeResult firstCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 7, 1, 0,0, 0,
                timeZone.toZoneId()), firstCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 7, 0, 0, 0,0,
                timeZone.toZoneId()), firstCalc.getNextCronFireTime());

        SchedulerComputeResult secCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 12, 1, 0,0, 0,
                timeZone.toZoneId()), secCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 12, 0, 0, 0,0,
                timeZone.toZoneId()), secCalc.getNextCronFireTime());

        SchedulerComputeResult thirdCalc = schedulerCalculator.computeFireTime();
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 13, 1, 0,0, 0,
                timeZone.toZoneId()), thirdCalc.getNextFireTime());
        Assert.assertEquals(ZonedDateTime.of(2018, 3, 13, 0, 0, 0,0,
                timeZone.toZoneId()), thirdCalc.getNextCronFireTime());
    }


}
