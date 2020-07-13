package com.github.cjmatta.ksql.functions;

import io.confluent.ksql.function.udaf.Udaf;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class DeltaTest {
    @Test
    public void deltaIntegers() {
        Udaf<Integer, Struct, Integer> udaf = Delta.deltaInt();
        Struct aggregate = udaf.initialize();

        Integer[] values = new Integer[] {5, 6, 4};
        for (Integer value: values) {
            aggregate = udaf.aggregate(value, aggregate);
        }
        Integer result = udaf.map(aggregate);

        assertEquals(-2, result);
    }

    @Test
    public void deltaLong() {
        Udaf<Long, Struct, Long> udaf = Delta.deltaLong();
        Struct aggregate = udaf.initialize();

        Long[] values = new Long[] {55L, 5L, 78L};
        for (Long value: values) {
            aggregate = udaf.aggregate(value, aggregate);
        }

        Long result = udaf.map(aggregate);

        assertEquals(73L, result);
    }

    @Test
    public void deltaDouble() {
        Udaf<Double, Struct, Double> udaf = Delta.deltaDouble();

        Struct aggregate = udaf.initialize();

        Double[] values = new Double[] {55.550, 5.323422, 78.644922};
        for (Double value: values) {
            aggregate = udaf.aggregate(value, aggregate);
        }

        Double result = udaf.map(aggregate);

        assertEquals(73.3215, result);
    }


}
