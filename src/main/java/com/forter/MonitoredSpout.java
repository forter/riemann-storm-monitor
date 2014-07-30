package com.forter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import java.util.List;
import java.util.Map;


/*
This class creates a monitored wrapper around other spout classes.
The usage is -
MonitoredSpout ms = new MonitoredSpout(new SpoutToMonitor());
*/
public class MonitoredSpout implements IRichSpout {
    private IRichSpout delegate;
    private String spoutService;

    public MonitoredSpout(IRichSpout delegate) {
        this.delegate = delegate;
    }

    @Override
    public void open(Map conf, final TopologyContext context, SpoutOutputCollector collector) {
        spoutService = context.getThisComponentId();
        delegate.open(conf, context, new SpoutOutputCollector(collector) {
            @Override
            public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
                List<Integer> emitResult = super.emit(streamId, tuple, messageId);
                Monitor.getMonitor().startLatency(messageId);
                return emitResult;
            }

            @Override
            public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
                super.emitDirect(taskId, streamId, tuple, messageId);
                Monitor.getMonitor().startLatency(messageId);
            }
        });
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void nextTuple() {
        delegate.nextTuple();
    }

    @Override
    public void ack(Object id) {
        Monitor.getMonitor().endLatency(id, spoutService, null /*error = null*/);
        delegate.ack(id);
    }

    @Override
    public void fail(Object id) {
        Monitor.getMonitor().endLatency(id, spoutService, new Throwable("Storm failed."));
        delegate.fail(id);
    }

    @Override
    public void activate() {
        delegate.activate();
    }

    @Override
    public void deactivate() {
        delegate.deactivate();
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