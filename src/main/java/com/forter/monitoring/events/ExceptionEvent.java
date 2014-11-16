package com.forter.monitoring.events;

import com.google.common.base.Throwables;

public class ExceptionEvent extends RiemannEvent {
    public ExceptionEvent(String exMessage) {
        super();
        this.description(exMessage);
        this.tags.add("exception");
    }

    public ExceptionEvent(Throwable t) {
        this(Throwables.getStackTraceAsString(t));
    }
}