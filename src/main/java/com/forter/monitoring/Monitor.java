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
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/*
This singleton class centralizes the storm-monitoring functions.
The monitored bolts and spouts will use the functions in this class.
 */
public class Monitor implements EventSender {
    private final EventSender eventSender;
    private final Map<Object, Long> startTimestampPerId;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final Map<String, String> customAttributes;
    private final Optional<String> metadataFieldName;

    public Monitor(Map conf, Optional<String> metadataFieldName) {
        this.metadataFieldName = metadataFieldName;
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
        this(new HashMap(), Optional.<String>absent());
    }

    public void send(RiemannEvent event) {
        event.attributes(customAttributes);

        if (event.tuple != null && metadataFieldName.isPresent() && event.tuple.contains(metadataFieldName.get())) {
            ObjectNode node = (ObjectNode) event.tuple.getValueByField(metadataFieldName.get());
            event.attributes(extractAttributesFromMetadataObject(node));
        }

        eventSender.send(event);
    }

    private Map<String, String> extractAttributesFromMetadataObject(ObjectNode node) {
        Map<String, String> attributes = Maps.newHashMap();

        if (node != null) {
            Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();
            while (iterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = iterator.next();
                attributes.put(entry.getKey(), entry.getValue().textValue());
            }
        }

        return attributes;
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
        if(startTimestampPerId.containsKey(latencyId)) {
            long elapsed = NANOSECONDS.toMillis(System.nanoTime() - startTimestampPerId.get(latencyId));

            LatencyEvent event = new LatencyEvent(elapsed).service(service).error(er);

            if (tuple != null) {
                event.tuple(tuple);
            }

            if (attributes != null) {
                event.attributes(attributes);
            }

            send(event);

            eventSender.send(event);

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