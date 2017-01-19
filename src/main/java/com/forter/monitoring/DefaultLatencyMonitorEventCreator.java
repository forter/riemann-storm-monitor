package com.forter.monitoring;

import backtype.storm.tuple.Tuple;
import com.forter.monitoring.events.ExceptionEvent;
import com.forter.monitoring.events.LatencyEvent;
import com.forter.monitoring.events.RiemannEvent;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class DefaultLatencyMonitorEventCreator implements LatencyMonitorEventCreator {
    public final static String MISSING_KEY_TAG = "latency-missing-key";
    public final static String UNEXPECTED_REMOVE_KEY_TAG = "latency-unexpectedly-removed";
    public final static DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    @Override
    public Iterable<RiemannEvent> createExpiryRemovalEvents(RemovalNotification<Object, Latencies> notification, String boltService) {
        RiemannEvent event = new ExceptionEvent("Latency object unexpectedly removed");
        event.attribute("removalCause", notification.getCause().name());
        event.tags(UNEXPECTED_REMOVE_KEY_TAG);
        event.service(boltService);
        if (notification.getValue() != null && notification.getValue().getTuple() != null) {
            final Tuple tuple = notification.getValue().getTuple();
            event.attribute("receivedFrom", tuple.getSourceComponent());
            if (notification.getCause() == RemovalCause.EXPIRED) {
                event.attribute("tuple", tuple.toString());
                event.tags("pii");
            }
        }
        return Lists.newArrayList(event);
    }

    @Override
    public Iterable<RiemannEvent> createMonitorKeyMissingEvents(String service, Object latencyId) {
        return Lists.newArrayList(new ExceptionEvent("Latency monitor doesn't recognize key.")
                .tags(MISSING_KEY_TAG)
                .service(service));
    }

    @Override
    public Iterable<RiemannEvent> createEmitLatencyEvents(long emitMillis, String boltService, Tuple tuple) {
        RiemannEvent event = new RiemannEvent()
                .metric(emitMillis)
                .tags("emit-latency")
                .service(boltService);

        if (tuple != null) {
            event.tuple(tuple);
        }

        return Lists.newArrayList(event);
    }

    @Override
    public Iterable<RiemannEvent> createLatencyEvents(Boolean success, Latencies latencies, long endTimeMillis, long elapsedMillis, EventProperties properties) {
        LatencyEvent event = (LatencyEvent) new LatencyEvent(elapsedMillis)
                .service(latencies.getService())
                .state(success ? "success" : "failure");

        if (!latencies.getHasFinished().get())
            latencies.getHasFinished().set(true);
        else {
            event.tags("strange-emit-error");
        }

        final long startTimeMillis = endTimeMillis - elapsedMillis;

        String startTime = DATE_FORMAT.format(startTimeMillis);

        if (latencies.getTuple() != null) {
            event.tuple(latencies.getTuple());
        }

        if (properties != null) {
            if (properties.getAttributes() != null) {
                event.attributes(properties.getAttributes());
            }
            if (properties.getTags() != null) {
                event.tags(properties.getTags());
            }
        }

        event.attribute("startTime", startTime);
        event.attribute("startTimeMillis", Long.toString(startTimeMillis));

        return Lists.<RiemannEvent>newArrayList(event);
    }
}
