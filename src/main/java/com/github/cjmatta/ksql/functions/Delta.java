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

    @UdafFactory(description = "Compute the delta between two consecutive Integers")
    public static Udaf<Integer, Struct, Integer> deltaInt() {

        final Schema AGGREGATE_STRUCT_SCHEMA = SchemaBuilder.struct().optional()
                .field(PREVIOUS, Schema.INT32_SCHEMA)
                .field(CURRENT, Schema.INT32_SCHEMA)
                .build();
        final Struct AGGREGATE_STRUCT = new Struct(AGGREGATE_STRUCT_SCHEMA);

        return new Udaf<Integer, Struct, Integer>() {

            @Override
            public Struct initialize() {
                AGGREGATE_STRUCT.put(PREVIOUS, 0);
                AGGREGATE_STRUCT.put(CURRENT, 0);
                return AGGREGATE_STRUCT;
            }

            @Override
            public Struct aggregate(Integer current, Struct aggregate) {
                AGGREGATE_STRUCT.put(PREVIOUS, aggregate.get(CURRENT));
                AGGREGATE_STRUCT.put(CURRENT, current);
                return AGGREGATE_STRUCT;
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

    @UdafFactory(description = "Compute the delta between two consecutive Longs")
    public static Udaf<Long, Struct, Long> deltaLong() {
        return new Udaf<Long, Struct, Long>() {
            final Schema AGGREGATE_STRUCT_SCHEMA = SchemaBuilder.struct().optional()
                    .field(PREVIOUS, Schema.INT64_SCHEMA)
                    .field(CURRENT, Schema.INT64_SCHEMA)
                    .build();
            final Struct AGGREGATE_STRUCT = new Struct(AGGREGATE_STRUCT_SCHEMA);

            @Override
            public Struct initialize() {
                AGGREGATE_STRUCT.put(CURRENT, 0L);
                AGGREGATE_STRUCT.put(PREVIOUS, 0L);
                return AGGREGATE_STRUCT;
            }

            @Override
            public Struct aggregate(Long current, Struct aggregate) {
                AGGREGATE_STRUCT.put(PREVIOUS, aggregate.getInt64(CURRENT));
                AGGREGATE_STRUCT.put(CURRENT, current);
                return AGGREGATE_STRUCT;
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
    
    @UdafFactory(description = "Compute the delta between two consecutive Doubles")
    public static Udaf<Double, Struct, Double> deltaDouble() {
        return new Udaf<Double, Struct, Double>() {
            final Schema AGGREGATE_STRUCT_SCHEMA = SchemaBuilder.struct().optional()
                    .field(PREVIOUS, Schema.FLOAT64_SCHEMA)
                    .field(CURRENT, Schema.FLOAT64_SCHEMA)
                    .build();
            final Struct AGGREGATE_STRUCT = new Struct(AGGREGATE_STRUCT_SCHEMA);

            @Override
            public Struct initialize() {
                AGGREGATE_STRUCT.put(CURRENT, 0.0);
                AGGREGATE_STRUCT.put(PREVIOUS, 0.0);
                return AGGREGATE_STRUCT;
            }

            @Override
            public Struct aggregate(Double current, Struct aggregate) {
                AGGREGATE_STRUCT.put(PREVIOUS, aggregate.getFloat64(CURRENT));
                AGGREGATE_STRUCT.put(CURRENT, current);
                return AGGREGATE_STRUCT;
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
