package com.forter.monitoring.eventSender;

import backtype.storm.tuple.Tuple;
import com.forter.monitoring.events.RiemannEvent;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.forter.monitoring.Monitor.parseAttributesString;

public class TupleAwareEventSender implements EventSender {
    private final static Logger logger = LoggerFactory.getLogger(TupleAwareEventSender.class);
    private final HashMap<String, String> environmentAttributes;

    private EventSender delegate;
    private transient Tuple currentTuple;

    public TupleAwareEventSender(EventSender delegate, Map conf) {
        this.delegate = delegate;
        this.environmentAttributes = Maps.newHashMap();
        if (conf.containsKey("topology.riemann.attributes")) {
            Object attributes = conf.get("topology.riemann.attributes");
            if (attributes instanceof String) {
                String attributesString = (String) attributes;
                environmentAttributes.putAll(parseAttributesString(attributesString));
            } else {
                if (logger == null) throw new RuntimeException("Logger is null");
                logger.warn("Wrong type of custom attributes for riemann, supposed to be String but is {}", attributes.getClass());
            }
        }
    }

    @Override
    public void send(RiemannEvent event) {
        if (currentTuple != null) {
            HashMap<String, String> attributes = Maps.newHashMap();

            for (String field : currentTuple.getFields()) {
                if (field.startsWith("_")) {
                    attributes.put(field.substring(1), String.valueOf(currentTuple.getValueByField(field)));
                }
            }

            event.tuple(currentTuple);

            if (!attributes.isEmpty()) {
                event.attributes(attributes);
            }
        }

        event.attributes(environmentAttributes);

        delegate.send(event);
    }

    public void setCurrentTuple(Tuple currentTuple) {
        this.currentTuple = currentTuple;
    }
}
