package com.rbkmoney.schedulator.serializer;

import com.rbkmoney.damsel.domain.BusinessScheduleRef;
import com.rbkmoney.damsel.domain.CalendarRef;
import com.rbkmoney.damsel.schedule.DominantBasedSchedule;
import com.rbkmoney.damsel.schedule.Schedule;
import com.rbkmoney.damsel.schedule.ScheduleJobRegistered;
import lombok.Data;

@Data
public class MachineRegisterState {

    private RegisterContext context;

    private String executorServicePath;

    private String schedulerId;

    private Long dominantRevisionId;

    private Integer businessSchedulerId;

    private Integer calendarId;

    public ScheduleJobRegistered mapToScheduleJobRegistered() {
        DominantBasedSchedule dominantBasedSchedule = new DominantBasedSchedule()
                .setBusinessScheduleRef(new BusinessScheduleRef(businessSchedulerId))
                .setCalendarRef(new CalendarRef(calendarId))
                .setRevision(dominantRevisionId);
        Schedule schedule = Schedule.dominant_schedule(dominantBasedSchedule);
        return new ScheduleJobRegistered()
                .setContext(context.getBytes())
                .setExecutorServicePath(executorServicePath)
                .setScheduleId(schedulerId)
                .setSchedule(schedule);
    }

}
