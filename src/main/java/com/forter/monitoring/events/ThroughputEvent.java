package com.forter.monitoring.events;


public class ThroughputEvent extends RiemannEvent {
    public ThroughputEvent() {
        super();
        this.metric = 1;
        this.tags.add("throughput");
    }

    @Override
    public ThroughputEvent service(String service) {
        this.service = service + " throughput.";
        return this;
    }
}
