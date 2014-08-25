package com.forter.monitoring;

import backtype.storm.tuple.TupleImpl;
import com.google.common.base.Throwables;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.*;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/*
* This class creates a monitored wrapper around other bolt classes to measure the time from execution till ack/fail.
* Currently ignores emit timings.
*/
public class MonitoredBolt implements IRichBolt {
    private static final int CHECK_BOLT_ACK_TUPLE_TREE_INTERVAL_SECS = 60;

    private final Class<? extends IComponent> delegateClass;
    private final IRichBolt delegate;

    private transient Logger logger;

    private String boltService;
    private Set<Integer> ackTupleSet = Sets.newHashSet();
    private Set<Integer> emitTupleSet = Sets.newHashSet();

    private final static ScheduledExecutorService timer;
    static {
        timer = new ScheduledThreadPoolExecutor(1);
    }

    private class MonitoredOutputCollector extends OutputCollector {
        MonitoredOutputCollector(IOutputCollector delegate) {
            super(delegate);
        }

        @Override
        public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            tickEmittedTuples(anchors);
            return super.emit(streamId, anchors, tuple);
        }

        @Override
        public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            tickEmittedTuples(anchors);
            super.emitDirect(taskId, streamId, anchors, tuple);
        }

        @Override
        public void ack(Tuple input) {
            tickDoneTuple(input);
            Monitor.getMonitor().endLatency(pair(input), boltService, null /*error = null*/ );
            super.ack(input);
        }

        @Override
        public void fail(Tuple input) {
            tickDoneTuple(input);
            Monitor.getMonitor().endLatency(pair(input), boltService, new Throwable(delegateClass.getCanonicalName() + " failed to process tuple") );
            super.fail(input);
        }

        @Override
        public void reportError(Throwable error) {
            Monitor.getMonitor().getEventSender().sendException(error, boltService);
            super.reportError(error);
        }

        private void tickEmittedTuples(Collection<Tuple> anchors) {
            if (anchors == null || anchors.isEmpty()) {
                String msg = String.format("%s bolt is emitting without anchors", boltService);
                Monitor.getMonitor().getEventSender().sendException(msg, boltService);
                logger.warn(msg);
            } else {
                for (Tuple t : anchors) {
                    if (emitTupleSet.contains(getTupleId(t))) {
                        emitTupleSet.remove(getTupleId(t));
                    } else {
                        // This is expected to happen in bolts that emit more than once
                        String msg = String.format("Emitted tuple from %s does not exist in the tuples collection", boltService);
                        Monitor.getMonitor().getEventSender().sendException(msg, boltService);
                        logger.debug(msg);
                    }
                }
            }
        }

        private void tickDoneTuple(Tuple input) {
            if (ackTupleSet.contains(getTupleId(input))) {
                ackTupleSet.remove(getTupleId(input));
            } else {
                String msg = String.format("Acking/Failing non-existent tuple in %s", boltService);
                Monitor.getMonitor().getEventSender().sendException(msg, boltService);
                logger.warn(msg);
            }
        }
    }

    public MonitoredBolt(IRichBolt delegate) {
        this.delegateClass = delegate.getClass();
        this.delegate = delegate;
    }

    public MonitoredBolt(IBasicBolt delegate) {
        this.delegateClass = delegate.getClass();
        this.delegate = new BasicBoltExecutor(delegate);
    }

    private static void injectEventSender(IRichBolt delegate) {
        if(delegate instanceof IEventSenderAware) {
            ((IEventSenderAware) delegate).setEventSender(Monitor.getMonitor().getEventSender());
        }
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        try {
            logger = LoggerFactory.getLogger(delegateClass);
            boltService = context.getThisComponentId();
            injectEventSender(delegate);
            delegate.prepare(conf, context, new MonitoredOutputCollector(collector));
        } catch(Throwable t) {
            logger.warn("Error during bolt prepare : ", t);
            throw Throwables.propagate(t);
        }
    }

    @Override
    public void execute(final Tuple tuple) {
        addToMonitoredTuples(tuple, ackTupleSet);
        addToMonitoredTuples(tuple, emitTupleSet);

        timer.schedule(new Runnable() {
            @Override
            public void run() {
                if (!ackTupleSet.isEmpty()) {
                    String msg = String.format("Bolt %s did not ack/fail in a timely fashion", boltService);
                    Monitor.getMonitor().getEventSender().sendException(msg, boltService);
                    logger.warn(msg);
                }
                if (!emitTupleSet.isEmpty()) {
                    // this is expected to happen in failed joins
                    String msg = String.format("Bolt %s did not anchor its emits to all the input tuples", boltService);
                    Monitor.getMonitor().getEventSender().sendException(msg, boltService);
                    logger.debug(msg);
                }
            }
        }, CHECK_BOLT_ACK_TUPLE_TREE_INTERVAL_SECS, TimeUnit.SECONDS);

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

    private void addToMonitoredTuples(Tuple tuple, Set<Integer> set) {
        if (set.contains(getTupleId(tuple))) {
            String msg = String.format("Got same tuple twice in %s", boltService);
            Monitor.getMonitor().getEventSender().sendException(msg, boltService);
            logger.warn(msg);
        } else {
            set.add(getTupleId(tuple));
        }
    }

    private Integer getTupleId(Tuple tuple) {
        if (tuple instanceof TupleImpl) {
            return tuple.getMessageId().hashCode();
        }
        return tuple.hashCode();
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
}



