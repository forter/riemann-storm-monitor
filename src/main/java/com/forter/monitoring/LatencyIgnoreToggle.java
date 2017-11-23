package com.forter.monitoring;

import org.apache.storm.tuple.Tuple;

public interface LatencyIgnoreToggle {
    boolean shouldIgnoreLatency(Tuple tuple, boolean isAck);
}
