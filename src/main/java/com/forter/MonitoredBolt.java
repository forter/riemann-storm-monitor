/*** Created by yaniv on 23/07/14.*/

package com.forter;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
 import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.Collection;
import java.util.List;
import java.util.Map;


public class MonitoredBolt implements IRichBolt {
    private IRichBolt delegate;

    public MonitoredBolt(IRichBolt delegate) {
        this.delegate = delegate;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            Monitor.startLatency(tuple.getMessageId());
            delegate.execute(tuple);
            Monitor.endLatency(tuple.getMessageId(), tuple.getSourceStreamId(), null);
        }
        catch(Exception er) {
            Monitor.endLatency(tuple.getMessageId(), tuple.getSourceStreamId(), new RuntimeException("Storm failed : " + er.getStackTrace()));
        }
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        if(Monitor.connection.client == null || !Monitor.connection.client.isConnected())
            Monitor.connection.connect();

        delegate.prepare(map, topologyContext, new OutputCollector(outputCollector) {
            @Override
            public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
                return super.emit(streamId, anchors, tuple);
            }

            @Override
            public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
                super.emitDirect(taskId, streamId, anchors, tuple);
            }
        });
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



