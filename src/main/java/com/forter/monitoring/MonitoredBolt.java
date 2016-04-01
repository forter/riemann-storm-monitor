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

/*
* This class creates a monitored wrapper around other bolt classes to measure the time from execution till ack/fail.
* Currently ignores emit timings.
*/
public abstract class MonitoredBolt implements IRichBolt {
    private final IRichBolt delegate;
    private final CustomLatencyAttributesGenerator customAttributesGenerator;
    private final boolean monitorLatency;

    transient String boltService;
    private transient Monitor monitor;

    private transient Logger logger;

    public MonitoredBolt(IRichBolt delegate) {
        this(delegate, null, true);
    }

    public MonitoredBolt(IRichBolt delegate, CustomLatencyAttributesGenerator customAttributesGenerator, boolean monitorLatency) {
        this.delegate = delegate;
        this.customAttributesGenerator = customAttributesGenerator;
        this.monitorLatency = monitorLatency;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        try {
            boltService = context.getThisComponentId();
            logger = LoggerFactory.getLogger(boltService);

            EventSender eventSender = getEventSender();
            monitor = new Monitor(conf, boltService, eventSender);

            if(delegate instanceof EventsAware) {
                ((EventsAware) delegate).setEventSender(eventSender);
            }

            if (this.monitorLatency) {
                delegate.prepare(conf, context, new MonitoredOutputCollector(this, collector));
            } else {
                delegate.prepare(conf, context, collector);
            }
        } catch(Throwable t) {
            logger.warn("Error during bolt prepare : ", t);
            throw Throwables.propagate(t);
        }
    }

    protected abstract EventSender getEventSender();

    @Override
    public void execute(Tuple tuple) {
        try {
            logger.trace("Entered execute with tuple: ", tuple);
            if (monitorLatency && monitor.shouldMonitor(tuple)) {
                if (delegate instanceof IgnoreLatencyComponent) {
                    if (!((IgnoreLatencyComponent) delegate).shouldMonitorLatency(tuple)) {
                        return;
                    }
                }
                monitor.startExecute(pair(tuple), tuple, this.boltService);
            }
        } finally {
            delegate.execute(tuple);
            logger.trace("Finished execution with tuple: ", tuple);
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
            logger.info("Error during bolt cleanup : ", t);
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
        monitor.send(event);
    }

    public CustomLatencyAttributesGenerator getCustomLatencyAttributesGenerator() {
        return customAttributesGenerator;
    }

    public Monitor getMonitor() {
        return monitor;
    }
}



