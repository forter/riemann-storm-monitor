package com.forter.monitoring;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.*;
import backtype.storm.tuple.Tuple;
import java.util.Collection;
import java.util.List;
import java.util.Map;


/*
This class creates a monitored wrapper around other bolt classes.
The usage is -
MonitoredBolt mb = new MonitoredBolt(new BoltToMonitor());
*/
public class MonitoredBolt implements IRichBolt {
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
            Monitor.getMonitor().endLatency(input.getMessageId(), boltService, null /*error = null*/ );
            super.ack(input);
        }

        @Override
        public void fail(Tuple input) {
            Monitor.getMonitor().endLatency(input.getMessageId(), boltService, new Throwable("Storm failed.") );
            super.fail(input);
        }
    }

    private IRichBolt delegate;
    private String boltService;

    public MonitoredBolt(IRichBolt delegate) {
        this.delegate = delegate;
    }
    public MonitoredBolt(IBasicBolt delegate) {
        this(new BasicBoltExecutor(delegate));
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        boltService = context.getThisComponentId();
        delegate.prepare(conf, context, new MonitoredOutputCollector(collector));
    }

    @Override
    public void execute(Tuple tuple) {
        Monitor.getMonitor().startLatency(tuple.getMessageId());
        delegate.execute(tuple);
    }

    @Override
    public void cleanup() {
        delegate.cleanup();
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



