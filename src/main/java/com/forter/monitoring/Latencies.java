package com.forter.monitoring;

import backtype.storm.tuple.Tuple;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import java.util.Map;

import static com.google.common.base.Optional.of;

/**
* Created by reem on 1/26/15.
*/
public class Latencies {
    private final Map<LatencyType, Latency> latencyMap;
    private final String service;
    private final Tuple tuple;

    public Latencies(Long executeStartNanos, String service, Tuple tuple) {
        this.latencyMap = Maps.newHashMap();
        this.setStartNanos(LatencyType.EXECUTE, executeStartNanos);
        this.service = service;
        this.tuple = tuple;
    }

    public Optional<Long> getLatencyNanos(LatencyType type) {
        final Latency latency = latencyMap.get(type);
        if (latency == null) {
            return Optional.absent();
        }
        if (latency.getStart() == null || latency.getEnd() == null) {
            return Optional.absent();
        }
        return of(latency.getEnd() - latency.getStart());
    }

    public void setStartNanos(LatencyType type, long nanos) {
        latencyMap.put(type, new Latency(nanos));
    }

    public boolean setEndNanos(LatencyType type, long nanos) {
        final Latency latency = latencyMap.get(type);
        if (latency != null) {
            latency.setEnd(nanos);
            return true;
        }
        return false;
    }

    public String getService() {
        return service;
    }

    public Tuple getTuple() {
        return tuple;
    }

    public static class Latency {
        private final Long start;
        private Long end;

        public Latency(Long start) {
            this.start = start;
        }

        public Long getStart() {
            return start;
        }

        public Long getEnd() {
            return end;
        }

        public void setEnd(Long end) {
            this.end = end;
        }
    }
}
