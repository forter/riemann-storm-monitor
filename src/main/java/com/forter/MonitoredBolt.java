/*** Created by yaniv on 23/07/14.*/

package com.forter;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import java.util.Map;


public class MonitoredBolt implements IBasicBolt {
    private IBasicBolt delegate;
    private String BoltService = "default";


    public MonitoredBolt(IBasicBolt delegate) {
        this.delegate = delegate;
    }

    @Override
    public void prepare(Map conf, TopologyContext context) {
        if(Monitor.connection.client == null || !Monitor.connection.client.isConnected())
            Monitor.connection.connect();

        BoltService = context.getThisComponentId();
        delegate.prepare(conf, context);
    }

    @Override
    public void execute(Tuple tuple, final BasicOutputCollector collector) {
        Monitor.startLatency(tuple.getMessageId());
        try {
            delegate.execute(tuple, collector);
            Monitor.endLatency(tuple.getMessageId(), this.BoltService, null);
        }
        catch(Exception er) {
            Monitor.endLatency(tuple.getMessageId(), this.BoltService, new RuntimeException("Storm failed : " + er.getStackTrace()));
        }
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



