package com.forter.monitoring;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forter.monitoring.eventSender.EventSender;
import com.forter.monitoring.eventSender.LoggerEventSender;
import com.forter.monitoring.eventSender.RiemannEventSender;
import com.forter.monitoring.events.ExceptionEvent;
import com.forter.monitoring.events.LatencyEvent;
import com.forter.monitoring.events.RiemannEvent;
import com.forter.monitoring.utils.RiemannDiscovery;
import com.google.common.base.Optional;
import com.google.common.cache.*;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/*
This singleton class centralizes the storm-monitoring functions.
The monitored bolts and spouts will use the functions in this class.
 */
public class Monitor implements EventSender {
    private final EventSender eventSender;
    private final Cache<Object, Long> startTimestampPerId;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final Map<String, String> customAttributes;

    public Monitor(Map conf, final String boltService) {
        startTimestampPerId = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(60, TimeUnit.MINUTES)
                .concurrencyLevel(2)
                .removalListener(new RemovalListener<Object, Long>() {
                    // The on removal callback is not instantly called on removal, but I  hope it will be called
                    // eventually. see:
                    // http://stackoverflow.com/questions/21986551/guava-cachebuilder-doesnt-call-removal-listener
                    @Override
                    public void onRemoval(RemovalNotification<Object, Long> notification) {
                        if (notification.getCause() != RemovalCause.EXPLICIT) {
                            ExceptionEvent event = new ExceptionEvent("Latency object unexpectedly removed");
                            event.attribute("removalCause", notification.getCause().name());
                            event.service(boltService);
                            send(event);
                        }
                    }
                })
                .build();
        if (RiemannDiscovery.getInstance().isAWS()) {
            eventSender = RiemannEventSender.getInstance();
        } else {
            //fallback for local mode
            eventSender = new LoggerEventSender();
        }
        customAttributes = extractCustomEventAttributes(conf);
    }

    public Monitor() {
        this(new HashMap(), "");
    }

    public void send(RiemannEvent event) {
        event.attributes(customAttributes);

        if (event.tuple != null) {
            HashMap<String, String> attributes = Maps.newHashMap();

            for (String field : event.tuple.getFields()) {
                if (field.startsWith("_")) {
                    attributes.put(field.substring(1), String.valueOf(event.tuple.getValueByField(field)));
                }
            }

            event.attributes(attributes);
        }

        eventSender.send(event);
    }

    public void startLatency(Object id) {
        final long now = System.nanoTime();

        startTimestampPerId.put(id, now);

        if (logger.isDebugEnabled()) {
            logger.debug("Monitoring latency for key {}", id);
        }
    }

    public void endSpoutLatency(Object latencyId, String service, Map<String, String> attributes, Throwable er) {
        endLatency(latencyId, service, null, attributes, er);
    }

    public void endLatency(Object latencyId, String service, Tuple tuple, Throwable er) {
        endLatency(latencyId, service, tuple, null, er);
    }

    public void endLatency(Object latencyId, String service, Tuple tuple, Map<String, String> attributes, Throwable er) {
        Long startTime = startTimestampPerId.getIfPresent(latencyId);
        if(startTime != null) {
            startTimestampPerId.invalidate(latencyId);

            long elapsed = NANOSECONDS.toMillis(System.nanoTime() - startTime);

            LatencyEvent event = new LatencyEvent(elapsed).service(service).error(er);

            if (tuple != null) {
                event.tuple(tuple);
            }

            if (attributes != null) {
                event.attributes(attributes);
            }

            event.attribute("startTime", Long.toString(startTime));

            send(event);

            if (logger.isDebugEnabled()) {
                logger.debug("Monitored latency {} for key {}", elapsed, latencyId);
            }
        } else {
            send(new ExceptionEvent("Latency monitor doesn't recognize key.").service(service));
            if (er == null) {
                logger.warn("Latency monitor doesn't recognize key {}.", latencyId);
            }
            else {
                send(new ExceptionEvent(er).service(service));
                logger.warn("Latency monitor doesn't recognize key {}. Swallowed exception {}", latencyId, er);
            }
        }
    }

    private Map<String,String> extractCustomEventAttributes(Map conf) {
        if (conf.containsKey("topology.riemann.attributes")) {
            Object attributes = conf.get("topology.riemann.attributes");
            if (attributes instanceof String) {
                String attributesString = (String) attributes;
                return parseAttributesString(attributesString);
            } else {
                logger.warn("Wrong type of custom attributes for riemann, supposed to be String but is {}", attributes.getClass());
            }
        }

        return new HashMap<String, String>();
    }

    private Map<String,String> parseAttributesString(String attributesString) {
        Map<String, String> attributesMap = new HashMap<>();

        for (String attribute : attributesString.split(",")) {
            String[] keyValue = attribute.split("=");
            if (keyValue.length != 2) {
                logger.warn("Bad format of custom attribute - {}", keyValue);
                continue;
            }
            attributesMap.put(keyValue[0], keyValue[1]);
        }
        return attributesMap;
    }

    /**
     * @return riemann client if discovered on aws.
     */
    public Optional<RiemannEventSender> getRiemannEventSender() {
        if (eventSender instanceof RiemannEventSender) {
            return Optional.of((RiemannEventSender)eventSender);
        }
        return Optional.absent();
    }
}