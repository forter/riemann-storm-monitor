package com.forter.monitoring.events;


public class LatencyEvent extends RiemannEvent {
    public Throwable error;

    public LatencyEvent(double latency) {
        super();
        this.metric = latency;
        this.state = "success";
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
}