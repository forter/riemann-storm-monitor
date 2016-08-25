package com.forter.monitoring;

import backtype.storm.tuple.Tuple;

public interface LatencyIgnoreToggle {
    boolean shouldIgnoreLatency(Tuple tuple, boolean isAck);
}
