package com.forter.monitoring;

import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.forter.monitoring.eventSender.EventsAware;
import com.forter.monitoring.events.ExceptionEvent;
import com.forter.monitoring.utils.PairKey;
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
    protected Optional<String> stormIdName;

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
            if(stormIdName.isPresent() && input.contains(stormIdName.get())) {
                String stormId = input.getStringByField(stormIdName.get());
                Monitor.getMonitor().endLatency(pair(input), boltService, stormIdName.get(), stormId, null /*error = null*/);
            } else {
                Monitor.getMonitor().endLatency(pair(input), boltService, /*error = */ null);
            }
            super.ack(input);
        }

        @Override
        public void fail(Tuple input) {
            if(stormIdName.isPresent() && input.contains(stormIdName.get())) {
                String stormId = input.getStringByField(stormIdName.get());
                Monitor.getMonitor().endLatency(pair(input), boltService, stormIdName.get(), stormId, new Throwable(boltService + " failed to process tuple") );
            } else {
                Monitor.getMonitor().endLatency(pair(input), boltService, new Throwable(boltService + " failed to process tuple"));
            }

            super.fail(input);
        }

        @Override
        public void reportError(Throwable error) {
            Monitor.getMonitor().getEventSender().send(new ExceptionEvent(error).service(boltService));
            super.reportError(error);
        }
    }

    public MonitoredBolt(IRichBolt delegate) {
        this.delegate = delegate;
    }

    private static void injectEventSender(IRichBolt delegate) {
        if(delegate instanceof EventsAware) {
            ((EventsAware) delegate).setEventSender(Monitor.getMonitor().getEventSender());
        }
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        try {
            boltService = context.getThisComponentId();
            logger = LoggerFactory.getLogger(boltService);
            injectEventSender(delegate);
            delegate.prepare(conf, context, new MonitoredOutputCollector(collector));
        } catch(Throwable t) {
            logger.warn("Error during bolt prepare : ", t);
            throw Throwables.propagate(t);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        logger.trace("Entered execute with tuple : ", tuple);
        Monitor.getMonitor().startLatency(pair(tuple));
        try {
            delegate.execute(tuple);
            logger.trace("Finished execution with tuple : ", tuple);
        } catch(Throwable t) {
            logger.info("Error during bolt execute : ", t);
            throw Throwables.propagate(t);
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

    /* A function to set the id name in the tuple.
         * The id name is a unique id to send via custom attributes to riemann,
         * which can later be used to filter events in various ways (riemann, kibana, etc)
         */
    public void setIdName(String idName) {
        stormIdName = Optional.of(idName);
    }

}



