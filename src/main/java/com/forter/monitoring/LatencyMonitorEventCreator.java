package com.forter.monitoring;

import org.apache.storm.tuple.Tuple;
import com.forter.monitoring.events.RiemannEvent;
import com.google.common.cache.RemovalNotification;

public interface LatencyMonitorEventCreator {
    Iterable<RiemannEvent> createExpiryRemovalEvents(RemovalNotification<Object, Latencies> notification, String boltService);
    Iterable<RiemannEvent> createMonitorKeyMissingEvents(String service, Object latencyId);
    Iterable<RiemannEvent> createEmitLatencyEvents(long emitMillis, String boltService, Tuple tuple);
    Iterable<RiemannEvent> createLatencyEvents(Boolean success, Latencies service, long endTimeMillis, long elapsedMillis, EventProperties properties);
}
