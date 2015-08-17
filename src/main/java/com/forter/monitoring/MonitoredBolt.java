package com.forter.monitoring;

import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.forter.monitoring.eventSender.*;
import com.forter.monitoring.events.ExceptionEvent;
import com.forter.monitoring.events.RiemannEvent;
import com.forter.monitoring.utils.PairKey;
import com.forter.monitoring.utils.RiemannDiscovery;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/*
* This class creates a monitored wrapper around other bolt classes to measure the time from execution till ack/fail.
* Currently ignores emit timings.
*/
public class MonitoredBolt implements IRichBolt {
    private final IRichBolt delegate;
    private transient Logger logger;
    private String boltService;
    private Monitor monitor;
    private TupleAwareEventSender tupleAwareEventSender;
    private TupleAwareEventSender injectedEventSender;

    private class MonitoredOutputCollector extends OutputCollector {
        MonitoredOutputCollector(IOutputCollector delegate) {
            super(delegate);
        }

        @Override
        public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            if (anchors != null) {
                for (Tuple t : anchors) {
                    monitor.startLatency(pair(t), LatencyType.EMIT);
                }
            }

            try {
                return super.emit(streamId, anchors, tuple);
            } finally {
                if (anchors != null) {
                    for (Tuple t : anchors) {
                        monitor.endLatency(pair(t), LatencyType.EMIT);
                    }
                }
            }
        }

        @Override
        public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            super.emitDirect(taskId, streamId, anchors, tuple);
        }

        @Override
        public void ack(Tuple input) {
            if (monitor.shouldMonitor(input)) {
                monitor.endExecute(pair(input), null, null);
            }
            super.ack(input);
        }

        @Override
        public void fail(Tuple input) {
            if (monitor.shouldMonitor(input)) {
                monitor.endExecute(pair(input), null, new Throwable(boltService + " failed to process tuple"));
            }
            super.fail(input);
        }

        @Override
        public void reportError(Throwable error) {
            Throwable t = error;

            if (t instanceof FailedException) {
                while (t instanceof FailedException && t.getCause() != null) {
                    t = t.getCause();
                }
            }

            monitor.send(new ExceptionEvent(t).service(boltService));
            super.reportError(t);
        }
    }

    public MonitoredBolt(IRichBolt delegate) {
        this.delegate = delegate;
    }

    private static void injectEventSender(IRichBolt delegate, EventSender eventSender) {
        if(delegate instanceof EventsAware) {
            ((EventsAware) delegate).setEventSender(new TupleAwareEventSender(eventSender));
        }
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        try {
            boltService = context.getThisComponentId();
            logger = LoggerFactory.getLogger(boltService);

            EventSender eventSender;
            if (injectedEventSender != null) {
                eventSender = injectedEventSender;
            } else if (RiemannDiscovery.getInstance().isAWS() && !RiemannDiscovery.getInstance().isJenkins()) {
                eventSender = RiemannEventSender.getInstance();
            } else {
                //fallback for local mode
                eventSender = new LoggerEventSender();
            }
            monitor = new Monitor(conf, boltService, eventSender);
            tupleAwareEventSender = new TupleAwareEventSender(monitor);
            injectEventSender(delegate, tupleAwareEventSender);

            delegate.prepare(conf, context, new MonitoredOutputCollector(collector));
        } catch(Throwable t) {
            logger.warn("Error during bolt prepare : ", t);
            throw Throwables.propagate(t);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        logger.trace("Entered execute with tuple: ", tuple);
        if (monitor.shouldMonitor(tuple)) {
            monitor.startExecute(pair(tuple), tuple, this.boltService);
        }
        tupleAwareEventSender.setCurrentTuple(tuple);
        delegate.execute(tuple);
        tupleAwareEventSender.setCurrentTuple(null);
        logger.trace("Finished execution with tuple: ", tuple);
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

    public void setInjectedEventSender(EventSender injectedEventSender) {
        this.injectedEventSender = new TupleAwareEventSender(injectedEventSender);
    }
}



