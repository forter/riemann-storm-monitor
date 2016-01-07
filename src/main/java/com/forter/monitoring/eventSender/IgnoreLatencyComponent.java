package com.forter.monitoring.eventSender;

import backtype.storm.tuple.Tuple;

public interface IgnoreLatencyComponent {
    boolean shouldMonitorLatency(Tuple tuple);
}
