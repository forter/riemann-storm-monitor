package com.forter.monitoring.events;

import com.google.common.collect.Lists;

import java.util.List;

public class LatencyEvent extends RiemannEvent {
    public Throwable error;

    public LatencyEvent(double latency) {
        super();
        this.metric = latency;
        this.tags.add("latency");
    }

    public LatencyEvent error(Throwable error) {
        this.error = error;
        this.state(error == null ? "success" : "failure");
        return this;
    }

    @Override
    public LatencyEvent service(String service) {
        this.service = service + " latency.";
        return this;
    }

    @Override
    public LatencyEvent tags(String... tags) {
        this.tags = Lists.newArrayList(tags);
        this.tags.add("latency");
        return this;
    }

    @Override
    public LatencyEvent tags(List<String> tags) {
        this.tags = tags;
        this.tags.add("latency");
        return this;
    }
}