package com.forter.monitoring;

import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forter.monitoring.eventSender.EventSender;
import com.forter.monitoring.eventSender.EventsAware;
import com.forter.monitoring.events.ExceptionEvent;
import com.forter.monitoring.events.RiemannEvent;
import com.google.common.base.Optional;
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
    protected Optional<String> metadataFieldName;

    private class MonitoredOutputCollector extends OutputCollector {

        MonitoredOutputCollector(IOutputCollector delegate) {
            super(delegate);
        }

        @Override
        public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            return super.emit(streamId, anchors, tuple);
        }

        @Override
        public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            super.emitDirect(taskId, streamId, anchors, tuple);
        }

        @Override
        public void ack(Tuple input) {
            monitor.endLatency(input.getMessageId(), boltService, input, null);
            super.ack(input);
        }

        @Override
        public void fail(Tuple input) {
            monitor.endLatency(input.getMessageId(), boltService, input, new Throwable(boltService + " failed to process tuple"));
            super.fail(input);
        }

        @Override
        public void reportError(Throwable error) {
            monitor.send(new ExceptionEvent(error).service(boltService));
            super.reportError(error);
        }
    }

    public MonitoredBolt(IRichBolt delegate) {
        this.delegate = delegate;
    }

    private static void injectEventSender(IRichBolt delegate, EventSender eventSender) {
        if(delegate instanceof EventsAware) {
            ((EventsAware) delegate).setEventSender(eventSender);
        }
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        try {
            boltService = context.getThisComponentId();
            logger = LoggerFactory.getLogger(boltService);
            monitor = new Monitor(conf, metadataFieldName);
            injectEventSender(delegate, monitor);
            delegate.prepare(conf, context, new MonitoredOutputCollector(collector));
        } catch(Throwable t) {
            logger.warn("Error during bolt prepare : ", t);
            throw Throwables.propagate(t);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        logger.trace("Entered execute with tuple : ", tuple);
        monitor.startLatency(tuple.getMessageId());
        try {
            delegate.execute(tuple);
            logger.trace("Finished execution with tuple : ", tuple);
        } catch(Throwable t) {
            logger.info("Error during bolt execute : ", t);
            throw Throwables.propagate(t);
        }
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

    /**
     * Sets the metadata field name in the tuple. The metadata contains various data that needs to be
     * reported to riemann on a per event basis.
     * @param metadataFieldName the field name to be set
     */
    public void setMetadataName(String metadataFieldName) {
        this.metadataFieldName = Optional.of(metadataFieldName);
    }
}



