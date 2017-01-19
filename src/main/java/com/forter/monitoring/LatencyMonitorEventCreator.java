package com.forter.monitoring;

import backtype.storm.tuple.Tuple;
import com.forter.monitoring.events.RiemannEvent;
import com.google.common.cache.RemovalNotification;

public interface LatencyMonitorEventCreator {
    Iterable<RiemannEvent> createExpiryRemovalEvents(RemovalNotification<Object, Latencies> notification, String boltService);
    Iterable<RiemannEvent> createMonitorKeyMissingEvents(String service, Object latencyId);
    Iterable<RiemannEvent> createEmitLatencyEvents(long emitMillis, String boltService, Tuple tuple);
    Iterable<RiemannEvent> createErrorEvents(Throwable er, String boltService);
}
