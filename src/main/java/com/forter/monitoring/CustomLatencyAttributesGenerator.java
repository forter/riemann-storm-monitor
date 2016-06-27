package com.forter.monitoring;

import org.apache.storm.tuple.Tuple;

public interface CustomLatencyAttributesGenerator {
    EventProperties getCustomAttributes(Tuple tuple);
}
