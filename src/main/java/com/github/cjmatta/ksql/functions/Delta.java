package com.github.cjmatta.ksql.functions;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;

@UdafDescription(name="delta", description = "Function that returns the delta between consecutive events with the same key")
public class Delta {
    @UdafFactory(description = "Compute the delta between two consecutive Integers")
    public static Udaf<Integer, Integer, Integer> deltaInt() {
        return new Udaf<Integer, Integer, Integer>() {

            @Override
            public Integer initialize() {
                return 0;
            }

            @Override
            public Integer aggregate(Integer current, Integer aggregate) {
                return current - aggregate;
            }

            @Override
            public Integer merge(Integer aggOne, Integer aggTwo) {
                return null;
            }

            @Override
            public Integer map(Integer agg) {
                return agg;
            }
        };
    }
    
    @UdafFactory(description = "Compute the delta between two consecutive Longs")
    public static Udaf<Long, Long, Long> deltaLong() {
        return new Udaf<Long, Long, Long>() {

            @Override
            public Long initialize() {
                return 0L;
            }

            @Override
            public Long aggregate(Long current, Long aggregate) {
                return current - aggregate;
            }

            @Override
            public Long merge(Long aggOne, Long aggTwo) {
                return null;
            }

            @Override
            public Long map(Long agg) {
                return agg;
            }
        };
    }
    
    @UdafFactory(description = "Compute the delta between two consecutive Doubles")
    public static Udaf<Double, Double, Double> deltaDouble() {
        return new Udaf<Double, Double, Double>() {

            @Override
            public Double initialize() {
                return 0.0;
            }

            @Override
            public Double aggregate(Double current, Double aggregate) {
                return current - aggregate;
            }

            @Override
            public Double merge(Double aggOne, Double aggTwo) {
                return null;
            }

            @Override
            public Double map(Double agg) {
                return agg;
            }
        };
    }
}
