package com.github.cjmatta.ksql.functions;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name="delta", description = "Function that returns the delta between consecutive events with the same key")
public class Delta {
    public static final String PREVIOUS = "PREVIOUS";
    public static final String CURRENT = "CURRENT";

    @UdafFactory(description = "Compute the delta between two consecutive Integers",
            aggregateSchema = "STRUCT<PREVIOUS integer, CURRENT integer>")
    public static Udaf<Integer, Struct, Integer> deltaInt() {

        final Schema AGGREGATE_STRUCT_SCHEMA = SchemaBuilder.struct().optional()
                .field(PREVIOUS, Schema.OPTIONAL_INT32_SCHEMA)
                .field(CURRENT, Schema.OPTIONAL_INT32_SCHEMA)
                .build();

        return new Udaf<Integer, Struct, Integer>() {

            @Override
            public Struct initialize() {
                return new Struct(AGGREGATE_STRUCT_SCHEMA)
                        .put(PREVIOUS, 0)
                        .put(CURRENT, 0);
            }

            @Override
            public Struct aggregate(Integer current, Struct aggregate) {
                return new Struct(AGGREGATE_STRUCT_SCHEMA)
                        .put(PREVIOUS, aggregate.get(CURRENT))
                        .put(CURRENT, current);
            }

            @Override
            public Struct merge(Struct aggOne, Struct aggTwo) {
                throw new RuntimeException("Session windows are unsupported with this UDAF");
            }

            @Override
            public Integer map(Struct agg) {
                return agg.getInt32(CURRENT) - agg.getInt32(PREVIOUS);
            }
        };
    }

    @UdafFactory(description = "Compute the delta between two consecutive Longs",
            aggregateSchema = "STRUCT<PREVIOUS bigint, CURRENT bigint>")
    public static Udaf<Long, Struct, Long> deltaLong() {
        return new Udaf<Long, Struct, Long>() {
            final Schema AGGREGATE_STRUCT_SCHEMA = SchemaBuilder.struct().optional()
                    .field(PREVIOUS, Schema.OPTIONAL_INT64_SCHEMA)
                    .field(CURRENT, Schema.OPTIONAL_INT64_SCHEMA)
                    .build();

            @Override
            public Struct initialize() {
                return new Struct(AGGREGATE_STRUCT_SCHEMA)
                        .put(PREVIOUS, 0L)
                        .put(CURRENT, 0L);
            }

            @Override
            public Struct aggregate(Long current, Struct aggregate) {
                return new Struct(AGGREGATE_STRUCT_SCHEMA)
                        .put(PREVIOUS, aggregate.get(CURRENT))
                        .put(CURRENT, current);
            }

            @Override
            public Struct merge(Struct aggOne, Struct aggTwo) {
                throw new RuntimeException("Session windows are unsupported with this UDAF");

            }

            @Override
            public Long map(Struct agg) {
                return agg.getInt64(CURRENT) - agg.getInt64(PREVIOUS);
            }
        };
    }
    
    @UdafFactory(description = "Compute the delta between two consecutive Doubles",
            aggregateSchema = "STRUCT<PREVIOUS double, CURRENT double>")
    public static Udaf<Double, Struct, Double> deltaDouble() {
        return new Udaf<Double, Struct, Double>() {
            final Schema AGGREGATE_STRUCT_SCHEMA = SchemaBuilder.struct().optional()
                    .field(PREVIOUS, Schema.OPTIONAL_FLOAT64_SCHEMA)
                    .field(CURRENT, Schema.OPTIONAL_FLOAT64_SCHEMA)
                    .build();

            @Override
            public Struct initialize() {
                return new Struct(AGGREGATE_STRUCT_SCHEMA)
                        .put(PREVIOUS, 0.0)
                        .put(CURRENT, 0.0);
            }

            @Override
            public Struct aggregate(Double current, Struct aggregate) {
                return new Struct(AGGREGATE_STRUCT_SCHEMA)
                        .put(PREVIOUS, aggregate.get(CURRENT))
                        .put(CURRENT, current);
            }

            @Override
            public Struct merge(Struct aggOne, Struct aggTwo) {
                throw new RuntimeException("Session windows are unsupported with this UDAF");

            }

            @Override
            public Double map(Struct agg) {
                return agg.getFloat64(CURRENT) - agg.getFloat64(PREVIOUS);
            }
        };
    }
}
