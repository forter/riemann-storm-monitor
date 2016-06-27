package com.forter.monitoring.eventSender;

import org.apache.storm.tuple.Tuple;
import com.forter.monitoring.events.RiemannEvent;
import com.google.common.collect.Maps;
import lombok.Setter;

import java.util.HashMap;

public class TupleAwareEventSender implements EventSender {
    private EventSender delegate;
    private transient Tuple currentTuple;

    public TupleAwareEventSender(EventSender delegate) {
        this.delegate = delegate;
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

        delegate.send(event);
    }

    public void setCurrentTuple(Tuple currentTuple) {
        this.currentTuple = currentTuple;
    }
}
