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

    public static SchedulatorMachineState mapToMachineState(ScheduleJobRegistered scheduleJobRegistered) {
        SchedulatorMachineState schedulatorMachineState = new SchedulatorMachineState();
        MachineRegisterState machineRegisterState = new MachineRegisterState();
        machineRegisterState.setContext(new RegisterContext(scheduleJobRegistered.getContext()));
        machineRegisterState.setExecutorServicePath(scheduleJobRegistered.getExecutorServicePath());
        machineRegisterState.setSchedulerId(scheduleJobRegistered.getScheduleId());
        machineRegisterState.setDominantRevisionId(scheduleJobRegistered.getSchedule().getDominantSchedule().getRevision());
        machineRegisterState.setBusinessSchedulerId(scheduleJobRegistered.getSchedule().getDominantSchedule().getBusinessScheduleRef().getId());
        machineRegisterState.setCalendarId(scheduleJobRegistered.getSchedule().getDominantSchedule().getCalendarRef().getId());
        schedulatorMachineState.setRegisterState(machineRegisterState);

        return schedulatorMachineState;
    }

}
