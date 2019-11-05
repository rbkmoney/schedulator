package com.rbkmoney.schedulator.util;

import com.cronutils.builder.CronBuilder;
import com.cronutils.mapper.ConstantsMapper;
import com.cronutils.mapper.WeekDay;
import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.field.CronFieldName;
import com.cronutils.model.field.expression.And;
import com.cronutils.model.field.expression.FieldExpression;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.rbkmoney.damsel.base.*;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.cronutils.model.field.CronFieldName.DAY_OF_MONTH;
import static com.cronutils.model.field.CronFieldName.DAY_OF_WEEK;
import static com.cronutils.model.field.expression.FieldExpression.always;
import static com.cronutils.model.field.expression.FieldExpression.questionMark;
import static com.cronutils.model.field.expression.FieldExpressionFactory.every;
import static com.cronutils.model.field.expression.FieldExpressionFactory.on;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SchedulerUtil {

    public static final CronParser QUARTZ_CRON_PARSER = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

    public static List<String> buildCron(Schedule schedule, Optional<DayOfWeek> firstDayOfWeek) {
        WeekDay weekDay = firstDayOfWeek
                .map(dayOfWeek -> new WeekDay(dayOfWeek.getValue(), false))
                .orElse(ConstantsMapper.JAVA8);

        if (schedule.getDayOfMonth().isSetEvery() && !schedule.getDayOfMonth().getEvery().isSetNth()) {
            if (schedule.getDayOfWeek().isSetEvery() && !schedule.getDayOfWeek().getEvery().isSetNth()) {
                return List.of(buildCron(schedule, weekDay, DAY_OF_WEEK));
            } else {
                return List.of(buildCron(schedule, weekDay, DAY_OF_MONTH));
            }
        }

        if (schedule.getDayOfWeek().isSetEvery() && !schedule.getDayOfWeek().getEvery().isSetNth()) {
            return List.of(buildCron(schedule, weekDay, DAY_OF_WEEK));
        } else {
            return List.of(
                    buildCron(schedule, weekDay, DAY_OF_WEEK),
                    buildCron(schedule, weekDay, DAY_OF_MONTH));
        }

    }

    public static String buildCron(Schedule schedule, WeekDay firstDayOfWeek, CronFieldName questionField) {
        CronBuilder cronBuilder = CronBuilder.cron(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ))
                .withYear(buildExpression(schedule.getYear()))
                .withMonth(buildExpression(schedule.getMonth()))
                .withDoM(buildExpression(schedule.getDayOfMonth()))
                .withDoW(buildExpression(schedule.getDayOfWeek(), firstDayOfWeek))
                .withHour(buildExpression(schedule.getHour()))
                .withMinute(buildExpression(schedule.getMinute()))
                .withSecond(buildExpression(schedule.getSecond()));

        switch (questionField) {
            case DAY_OF_WEEK:
                cronBuilder.withDoW(questionMark());
                break;
            case DAY_OF_MONTH:
                cronBuilder.withDoM(questionMark());
                break;
            default:
                throw new IllegalArgumentException("'?' can only be specified for Day-of-Month or Day-of-Week");
        }
        return cronBuilder.instance().asString();
    }

    private static FieldExpression buildScheduleEveryExpression(ScheduleEvery scheduleEvery) {
        FieldExpression fieldExpression;
        if (scheduleEvery.isSetNth()) {
            fieldExpression = every(scheduleEvery.getNth());
        } else {
            fieldExpression = always();
        }
        return fieldExpression;
    }

    private static FieldExpression buildDaysOfWeekOnExpression(Set<DayOfWeek> days, WeekDay firstDayOfWeek) {
        Set<Integer> dayValues = days.stream()
                .map(dayValue -> ConstantsMapper.weekDayMapping(firstDayOfWeek, ConstantsMapper.QUARTZ_WEEK_DAY, dayValue.getValue()))
                .collect(Collectors.toSet());
        return buildOnExpression(dayValues);
    }

    private static FieldExpression buildMonthOnExpression(Set<Month> months) {
        Set<Integer> monthValues = months.stream()
                .map(Month::getValue)
                .collect(Collectors.toSet());
        return buildOnExpression(monthValues);
    }

    private static FieldExpression buildOnExpression(Set<Integer> times) {
        if (times.isEmpty()) {
            throw new IllegalArgumentException("Expression 'On' must not be empty");
        }
        FieldExpression fieldExpression = new And();
        for (int value : times) {
            fieldExpression.and(on(value));
        }
        return fieldExpression;
    }

    private static FieldExpression buildExpression(ScheduleYear scheduleYear) {
        ScheduleYear._Fields field = scheduleYear.getSetField();
        switch (field) {
            case EVERY:
                return buildScheduleEveryExpression(scheduleYear.getEvery());
            case ON:
                return buildOnExpression(scheduleYear.getOn());
            default:
                throw new IllegalArgumentException(String.format("Unknown ScheduleYear field, field='%s'", field));
        }
    }

    private static FieldExpression buildExpression(ScheduleMonth scheduleMonth) {
        ScheduleMonth._Fields field = scheduleMonth.getSetField();
        switch (field) {
            case EVERY:
                return buildScheduleEveryExpression(scheduleMonth.getEvery());
            case ON:
                return buildMonthOnExpression(scheduleMonth.getOn());
            default:
                throw new IllegalArgumentException(String.format("Unknown ScheduleMonth field, field='%s'", field));
        }
    }

    private static FieldExpression buildExpression(ScheduleDayOfWeek scheduleDayOfWeek, WeekDay firstDayOfWeek) {
        ScheduleDayOfWeek._Fields field = scheduleDayOfWeek.getSetField();
        switch (field) {
            case EVERY:
                return buildScheduleEveryExpression(scheduleDayOfWeek.getEvery());
            case ON:
                return buildDaysOfWeekOnExpression(scheduleDayOfWeek.getOn(), firstDayOfWeek);
            default:
                throw new IllegalArgumentException(String.format("Unknown DayOfWeek field, field='%s'", field));
        }
    }

    private static FieldExpression buildExpression(ScheduleFragment scheduleFragment) {
        ScheduleFragment._Fields field = scheduleFragment.getSetField();
        switch (field) {
            case EVERY:
                return buildScheduleEveryExpression(scheduleFragment.getEvery());
            case ON:
                return buildOnExpression(
                        scheduleFragment.getOn().stream()
                                .map(Byte::intValue)
                                .collect(Collectors.toSet())
                );
            default:
                throw new IllegalArgumentException(String.format("Unknown ScheduleFragment field, field='%s'", field));
        }
    }

    public static String getNearestCron(List<String> cronExpressionList, ZonedDateTime dateTime) {
        String nearestCronExpression = null;
        for (String cronExpression : cronExpressionList) {
            if (nearestCronExpression == null) {
                nearestCronExpression = cronExpression;
                continue;
            }

            Cron nearestCron = QUARTZ_CRON_PARSER.parse(nearestCronExpression).validate();
            Cron cron = QUARTZ_CRON_PARSER.parse(cronExpression).validate();
            Optional<ZonedDateTime> nearestCronNextExec = ExecutionTime.forCron(nearestCron).nextExecution(dateTime);
            Optional<ZonedDateTime> cronNextExec = ExecutionTime.forCron(cron).nextExecution(dateTime);

            if (nearestCronNextExec.isPresent() && cronNextExec.isPresent()) {
                int res = nearestCronNextExec.get().compareTo(cronNextExec.get());
                if (res > 0) {
                    nearestCronExpression = cronExpression;
                }
            }
        }

        return nearestCronExpression;
    }

}
