package com.rbkmoney.schedulator.backoff;

import org.junit.Assert;
import org.junit.Test;

public class JobExponentialBackOffTest {

    @Test
    public void testNextBackOff() {
        int initialInterval = 30;
        JobExponentialBackOff jobExponentialBackOff = new JobExponentialBackOff(3600, initialInterval, -1);
        long firstBackOff = jobExponentialBackOff.nextBackOff();
        Assert.assertEquals(initialInterval, firstBackOff);
        long secBackOff = jobExponentialBackOff.nextBackOff();
        long multiplyedFirstbackOff = firstBackOff * 2;
        Assert.assertEquals(multiplyedFirstbackOff, secBackOff);
    }

    @Test
    public void testMaxBackOff() {
        JobExponentialBackOff jobExponentialBackOff = new JobExponentialBackOff(3600, 30, 3000);
        long nextBackOff = jobExponentialBackOff.nextBackOff();
        Assert.assertEquals(3600, nextBackOff);
    }

}
