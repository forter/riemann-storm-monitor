package com.forter.monitoring.eventSender;

import org.apache.storm.tuple.Tuple;

public interface IgnoreLatencyComponent {
    boolean shouldMonitorLatency(Tuple tuple);
}
