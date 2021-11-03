package com.rbkmoney.schedulator.serializer;

import com.rbkmoney.damsel.schedule.ScheduleJobRegistered;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SchedulatorMachineState {

    private MachineRegisterState registerState;

    private MachineTimerState timerState;

    public SchedulatorMachineState(ScheduleJobRegistered scheduleJobRegistered) {
        MachineRegisterState registerState = new MachineRegisterState();
        registerState.setContext(new RegisterContext(scheduleJobRegistered.getContext()));
        registerState.setExecutorServicePath(scheduleJobRegistered.getExecutorServicePath());
        registerState.setSchedulerId(scheduleJobRegistered.getScheduleId());
        registerState.setDominantRevisionId(scheduleJobRegistered.getSchedule().getDominantSchedule().getRevision());
        registerState.setBusinessSchedulerId(
                scheduleJobRegistered.getSchedule().getDominantSchedule().getBusinessScheduleRef().getId());
        registerState.setCalendarId(scheduleJobRegistered.getSchedule().getDominantSchedule().getCalendarRef().getId());
        this.registerState = registerState;
    }

}
