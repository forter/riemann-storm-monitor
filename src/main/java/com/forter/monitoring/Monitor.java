package com.forter.monitoring;
import com.forter.monitoring.eventSender.EventSender;
import com.forter.monitoring.eventSender.LoggerEventSender;
import com.forter.monitoring.events.ExceptionEvent;
import com.forter.monitoring.events.LatencyEvent;
import com.forter.monitoring.events.RiemannEvent;
import com.forter.monitoring.utils.RiemannDiscovery;
import com.forter.monitoring.eventSender.RiemannEventSender;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/*
This singleton class centralizes the storm-monitoring functions.
The monitored bolts and spouts will use the functions in this class.
 */
class Monitor implements EventSender {
    private final EventSender eventSender;
    private final Map<Object, Long> startTimestampPerId;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final Map<String, String> customAttributes;

    public Monitor(Map conf) {
        startTimestampPerId = Maps.newConcurrentMap();
        if (RiemannDiscovery.getInstance().isAWS()) {
            eventSender = RiemannEventSender.getInstance();
        } else {
            //fallback for local mode
            eventSender = new LoggerEventSender();
        }
        customAttributes = extractCustomEventAttributes(conf);
    }

    public Monitor() {
        this(new HashMap());
    }

    public void send(RiemannEvent event) {
        event.attributes(customAttributes);
        eventSender.send(event);
    }

    public void startLatency(Object id) {
        final long now = System.nanoTime();

        startTimestampPerId.put(id, now);

        if (logger.isDebugEnabled()) {
            logger.debug("Monitoring latency for key {}", id);
        }
    }

    public void endLatency(Object latencyId, String service, Throwable er) {
        endLatency(latencyId, service, null, null, er);
    }

    public void endLatency(Object latencyId, String service, String stormIdName, String stormIdValue, Throwable er) {
        if(startTimestampPerId.containsKey(latencyId)) {
            long elapsed = NANOSECONDS.toMillis(System.nanoTime() - startTimestampPerId.get(latencyId));

            LatencyEvent event = new LatencyEvent(elapsed).service(service).error(er);

            if(stormIdName != null && stormIdValue != null) {
                event.attribute(stormIdName, stormIdValue);
            }
            send(event);

            startTimestampPerId.remove(latencyId);
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
        Map<String, String> attributesMap = new HashMap<String, String>();

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
}