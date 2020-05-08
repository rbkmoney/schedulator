package com.rbkmoney.schedulator.backoff;

public class JobExponentialBackOff implements JobBackOff {

    private static final long DEFAULT_INITIAL_INTERVAL = 30;

    private static final long DEFAULT_MAX_ELAPSED_TIME = Long.MAX_VALUE;

    private static final double DEFAULT_MULTIPLIER = 2;

    private long maxInterval;

    private long initialInterval;

    private long currentInterval;

    public JobExponentialBackOff(long maxInterval, long initialInterval, long currentInterval) {
        this.maxInterval = maxInterval;
        this.initialInterval = initialInterval;
        this.currentInterval = currentInterval;
    }

    @Override
    public long nextBackOff() {
        if (this.currentInterval >= DEFAULT_MAX_ELAPSED_TIME) {
            return -1;
        }

        return computeNextInterval();
    }

    private long computeNextInterval() {
        if (this.currentInterval >= maxInterval) {
            return maxInterval;
        } else if (this.currentInterval < 0) {
            long interval = initialInterval <= 0 ? DEFAULT_INITIAL_INTERVAL : initialInterval;
            this.currentInterval = (Math.min(interval, maxInterval));
        } else {
            this.currentInterval = multiplyInterval(maxInterval);
        }
        return this.currentInterval;
    }

    private long multiplyInterval(long maxInterval) {
        long i = this.currentInterval;
        i *= DEFAULT_MULTIPLIER;
        return (Math.min(i, maxInterval));
    }

}
