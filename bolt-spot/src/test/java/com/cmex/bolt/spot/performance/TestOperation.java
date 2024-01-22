package com.cmex.bolt.spot.performance;

import com.google.common.base.Stopwatch;
import org.decimal4j.immutable.Decimal18f;
import org.decimal4j.immutable.Decimal8f;
import org.decimal4j.mutable.MutableDecimal8f;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

public class TestOperation {

    @Test
    public void testAdd() {
        long sum = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (long i = 0; i < 100000000; i++) {
            sum += i;
        }
        System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
        System.out.println(sum);
        double dsum = 0.0;
        stopwatch.reset();
        stopwatch.start();
        for (long i = 0; i < 100000000; i++) {
            dsum += i;
        }
        System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
        System.out.println(dsum);
        stopwatch.reset();
        stopwatch.start();
        BigDecimal bsum = BigDecimal.ZERO;
        for (long i = 0; i < 100000000; i++) {
            bsum = bsum.add(new BigDecimal(i));
        }
        System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
        System.out.println(bsum.toPlainString());
    }

    @Test
    public void testElapsed() throws InterruptedException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        TimeUnit.SECONDS.sleep(1);
        System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
        stopwatch.reset();
        stopwatch.start();
        TimeUnit.SECONDS.sleep(1);
        System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));

    }
}
