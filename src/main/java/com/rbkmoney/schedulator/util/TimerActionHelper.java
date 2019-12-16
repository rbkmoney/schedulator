package com.rbkmoney.schedulator.util;

import com.rbkmoney.machinegun.base.Timer;
import com.rbkmoney.machinegun.stateproc.ComplexAction;
import com.rbkmoney.machinegun.stateproc.RemoveAction;
import com.rbkmoney.machinegun.stateproc.SetTimerAction;
import com.rbkmoney.machinegun.stateproc.TimerAction;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class TimerActionHelper {

    public static ComplexAction buildTimerAction(String deadline) {
        ComplexAction complexAction = new ComplexAction();
        TimerAction timer = new TimerAction();
        SetTimerAction setTimerAction = new SetTimerAction();
        setTimerAction.setTimer(Timer.deadline(deadline));
        timer.setSetTimer(setTimerAction);
        complexAction.setTimer(timer);
        return complexAction;
    }

    public static ComplexAction buildRemoveAction() {
        ComplexAction complexAction = new ComplexAction();
        RemoveAction removeAction = new RemoveAction();
        complexAction.setRemove(removeAction);

        return complexAction;
    }

}
