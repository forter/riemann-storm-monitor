/*** Created by yaniv on 23/07/14.*/

package com.forter;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import java.util.List;
import java.util.Map;

public class MonitoredSpout implements IRichSpout {
    private IRichSpout delegate;

    public MonitoredSpout(IRichSpout delegate) {
        this.delegate = delegate;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        if(Monitor.connection.client == null || !Monitor.connection.client.isConnected())
            Monitor.connection.connect();

        delegate.open(conf, context, new SpoutOutputCollector(collector) {
            @Override
            public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
                Monitor.startLatency(messageId);
                return super.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
            }

            @Override
            public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
                Monitor.startLatency(messageId);
                super.emitDirect(taskId, streamId, tuple, newStreamMessageId(streamId, messageId));
            }
            private MonitoringMessage newStreamMessageId(String stream, Object messageId) {
                return new MonitoringMessage(messageId, stream);
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
        Monitor.endLatency(((MonitoringMessage) id).id, ((MonitoringMessage)id).service, null);
        delegate.ack(id);
    }

    @Override
    public void fail(Object id) {
        Monitor.endLatency(((MonitoringMessage)id).id, ((MonitoringMessage)id).service, new RuntimeException("Storm failed."));
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