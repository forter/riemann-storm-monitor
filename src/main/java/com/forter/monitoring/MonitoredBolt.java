package com.forter.monitoring;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.forter.monitoring.eventSender.EventSender;
import com.forter.monitoring.eventSender.EventsAware;
import com.forter.monitoring.eventSender.IgnoreLatencyComponent;
import com.forter.monitoring.events.RiemannEvent;
import com.forter.monitoring.utils.PairKey;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/*
* This class creates a monitored wrapper around other bolt classes to measure the time from execution till ack/fail.
* Currently ignores emit timings.
*/
public abstract class MonitoredBolt implements IRichBolt {
    private static final long THROUGHPUT_REPORT_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(10);

    private final IRichBolt delegate;
    private final int latencyFraction;
    private final boolean monitorThroughput;

    private CustomLatencyAttributesGenerator customAttributesGenerator;
    private LatencyIgnoreToggle latencyIgnoreToggle;

    transient String componentId;
    private transient Logger logger;

    private transient Monitor monitor;
    private transient long lastThroughputSent;
    private transient long executedInCycle;
    private LatencyMonitorEventCreator latencyRemovalEventCreator;

    public MonitoredBolt(IRichBolt delegate) {
        this(delegate, 1, false);
    }

    public MonitoredBolt(IRichBolt delegate, int latencyFraction, boolean monitorThroughput) {
        this.delegate = delegate;
        this.latencyFraction = latencyFraction;
        this.monitorThroughput = monitorThroughput;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        try {
            componentId = context.getThisComponentId();
            logger = LoggerFactory.getLogger(componentId);

            EventSender eventSender = getEventSender();
            monitor = new Monitor(conf, componentId, eventSender, latencyRemovalEventCreator);

            if(delegate instanceof EventsAware) {
                ((EventsAware) delegate).setEventSender(eventSender);
            }

            delegate.prepare(conf, context, wrapCollector(collector, context));

            this.lastThroughputSent = System.currentTimeMillis();
            this.executedInCycle = 0L;
        } catch(Throwable t) {
            logger.warn("Error during bolt prepare: ", t);
            throw Throwables.propagate(t);
        }
    }

    protected OutputCollector wrapCollector(OutputCollector collector, TopologyContext context) {
        return new MonitoredOutputCollector(this, collector, this.latencyFraction);
    }

    protected abstract EventSender getEventSender();

    @Override
    public void execute(Tuple tuple) {
        try {
            logger.trace("Entered execute with tuple: ", tuple);
            if (monitor.shouldMonitor(tuple)) {
                if (delegate instanceof IgnoreLatencyComponent) {
                    if (!((IgnoreLatencyComponent) delegate).shouldMonitorLatency(tuple)) {
                        return;
                    }
                }
                if (tuple.hashCode() % this.latencyFraction == 0) {
                    monitor.startExecute(pair(tuple), tuple, this.componentId);
                }
            }
        } finally {
            delegate.execute(tuple);
            logger.trace("Finished execution with tuple: ", tuple);
        }

        if (this.monitorThroughput) {
            this.executedInCycle++;
            if ((System.currentTimeMillis() - this.lastThroughputSent) > THROUGHPUT_REPORT_INTERVAL_MILLIS) {
                try {
                    monitor.send(new RiemannEvent()
                            .metric(this.executedInCycle)
                            .service(this.componentId + " latency. count")
                            .tuple(tuple));
                } finally {
                    this.executedInCycle = 0L;
                    this.lastThroughputSent = System.currentTimeMillis();
                }
            }
        }
    }

    private PairKey pair(Tuple tuple) {
        return new PairKey(this, tuple);
    }

    @Override
    public void cleanup() {
        try {
            delegate.cleanup();
        } catch(Throwable t) {
            logger.info("Error during bolt cleanup: ", t);
            throw Throwables.propagate(t);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        delegate.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return delegate.getComponentConfiguration();
    }

    public void send(RiemannEvent event) {
        if (monitor != null) monitor.send(event);
    }

    CustomLatencyAttributesGenerator getCustomLatencyAttributesGenerator() {
        return customAttributesGenerator;
    }

    public void setCustomLatencyAttributesGenerator(CustomLatencyAttributesGenerator customAttributesGenerator) {
        this.customAttributesGenerator = customAttributesGenerator;
    }

    LatencyIgnoreToggle getLatencyIgnoreToggle() {
        return latencyIgnoreToggle;
    }

    public void setLatencyIgnoreToggle(LatencyIgnoreToggle latencyIgnoreToggle) {
        this.latencyIgnoreToggle = latencyIgnoreToggle;
    }

    public Monitor getMonitor() {
        return monitor;
    }

    public void setLatencyRemovalEventCreator(LatencyMonitorEventCreator latencyRemovalEventCreator) {
        this.latencyRemovalEventCreator = latencyRemovalEventCreator;
    }
}



