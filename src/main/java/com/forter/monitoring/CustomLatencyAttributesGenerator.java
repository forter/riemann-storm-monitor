package com.forter.monitoring;

import backtype.storm.tuple.Tuple;

public interface CustomLatencyAttributesGenerator {
    EventProperties getCustomAttributes(Tuple tuple);
}
