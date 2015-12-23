package com.forter.monitoring.eventSender;

import backtype.storm.tuple.Tuple;
import com.forter.monitoring.events.RiemannEvent;
import lombok.Setter;

public class TupleAwareEventSender implements EventSender {
    private EventSender delegate;
    private transient Tuple currentTuple;

    public TupleAwareEventSender(EventSender delegate) {
        this.delegate = delegate;
    }

    @Override
    public void send(RiemannEvent event) {
        event.tuple(currentTuple);
        delegate.send(event);
    }

    public void setCurrentTuple(Tuple currentTuple) {
        this.currentTuple = currentTuple;
    }
}
