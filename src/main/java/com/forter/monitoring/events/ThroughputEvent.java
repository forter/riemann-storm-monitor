package com.forter.monitoring.events;


public class ThroughputEvent extends RiemannEvent {
    public ThroughputEvent() {
        super();
        this.metric = 1;
        this.tags.add("throughput");
    }

    //A function to make sure the metric doesn't change
    @Override
    public ThroughputEvent metric(double metric) {
        if(metric != 1) {
            throw new IllegalArgumentException("The metric for a throughput event must be 1.");
        }
        this.metric = metric;
        return this;
    }

    @Override
    public ThroughputEvent service(String service) {
        this.service = service + " throughput.";
        return this;
    }
}
