package com.forter.monitoring.events;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import java.util.List;

public class ExceptionEvent extends RiemannEvent {
    public ExceptionEvent(String exMessage) {
        super();
        this.description(exMessage);
        this.tags.add("uncaught-exception");
    }

    public ExceptionEvent(Throwable t) {
        this(Throwables.getStackTraceAsString(t));
    }

    @Override
    public ExceptionEvent tags(String ... tags) {
        this.tags = Lists.newArrayList(tags);
        this.tags.add("uncaught-exception");
        return this;
    }

    @Override
    public ExceptionEvent tags(List<String> tags) {
        this.tags = tags;
        this.tags.add("uncaught-exception");
        return this;
    }
}