package com.forter.monitoring;

import com.google.common.base.Throwables;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.*;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.List;
import java.util.Map;


/*
This class creates a monitored wrapper around other bolt classes to measure the time from execution till ack/fail.
Currently ignores emit timings.
*/
public class MonitoredBolt implements IRichBolt {
    private final Class<? extends IComponent> delegateClass;
    private final IRichBolt delegate;
    private String boltService;
    private final Logger logger;

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
            Monitor.getMonitor().endLatency(input, boltService, null /*error = null*/ );
            super.ack(input);
        }

        @Override
        public void fail(Tuple input) {
            Monitor.getMonitor().endLatency(input, boltService, new Throwable(delegateClass.getCanonicalName() + " failed to process tuple") );
            super.fail(input);
        }

        @Override
        public void reportError(Throwable error) {
            Monitor.getMonitor().getEventSender().sendException(new Throwable(delegateClass.getCanonicalName() + " failed to process tuple"), boltService);
            super.reportError(error);
        }
    }

    public MonitoredBolt(IRichBolt delegate) {
        this.delegateClass = delegate.getClass();
        this.delegate = delegate;
        this.logger = LoggerFactory.getLogger(delegate.getClass());
    }

    public MonitoredBolt(IBasicBolt delegate) {
        this(new BasicBoltExecutor(delegate));
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        try {
            boltService = context.getThisComponentId();
            delegate.prepare(conf, context, new MonitoredOutputCollector(collector));
        } catch(Throwable t) {
            logger.info("Error during bolt prepare : ", t);
            throw Throwables.propagate(t);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        Monitor.getMonitor().startLatency(tuple);
        try {
            delegate.execute(tuple);
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
}



