package com.rbkmoney.schedulator.util;

import com.rbkmoney.machinegun.base.Timer;
import com.rbkmoney.machinegun.stateproc.*;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class TimerActionHelper {

    public static ComplexAction buildTimerAction(String deadline, HistoryRange historyRange) {
        ComplexAction complexAction = new ComplexAction();
        TimerAction timer = new TimerAction();
        SetTimerAction setTimerAction = new SetTimerAction();
        setTimerAction.setTimer(Timer.deadline(deadline));
        setTimerAction.setRange(historyRange);
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

    public static HistoryRange buildLastEventHistoryRange() {
        HistoryRange historyRange = new HistoryRange();
        historyRange.setDirection(Direction.backward);
        historyRange.setLimit(1);
        return historyRange;
    }

}
