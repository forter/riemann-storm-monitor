package com.forter.monitoring.eventSender;

import backtype.storm.tuple.Tuple;
import com.forter.monitoring.events.RiemannEvent;
import lombok.Setter;

public class TupleAwareEventSender implements EventSender{
    private EventSender delegate;
    @Setter
    private transient Tuple currentTuple;

    public TupleAwareEventSender(EventSender delegate) {
        this.delegate = delegate;
    }

    @Override
    public void send(RiemannEvent event) {
        event.tuple(currentTuple);
        delegate.send(event);
    }
}
