package com.github.cjmatta.ksql.functions;

import io.confluent.ksql.function.udaf.Udaf;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class DeltaTest {
    @Test
    public void deltaIntegers() {
        Udaf<Integer, Integer, Integer> udaf = Delta.deltaInt();

        Integer aggregate = udaf.aggregate(4, 5);
        assertEquals(-1, aggregate);
    }

    @Test
    public void deltaLong() {
        Udaf<Long, Long, Long> udaf = Delta.deltaLong();

        Long aggregate = udaf.aggregate(44L, 5L);
        assertEquals(39L, aggregate);
    }

    @Test
    public void deltaDouble() {
        Udaf<Double, Double, Double> udaf = Delta.deltaDouble();

        Double aggregate = udaf.aggregate(44.005, 5.0);
        assertEquals(39.005, aggregate);
    }


}
