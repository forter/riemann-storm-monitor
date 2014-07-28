/*** Created by yaniv on 23/07/14.*/

package com.forter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.Map;


public class MonitoredBolt implements IRichBolt {
    private IRichBolt delegate;

    public MonitoredBolt(IRichBolt delegate, RiemannConnection connection) {
        this.delegate = delegate;
    }

    @Override
    public void execute(Tuple tuple) {
        //start latency
        delegate.execute(tuple);
        //en

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        if(!Monitor.connection.client.isConnected())
            Monitor.connection.connect();

        delegate.prepare(map, topologyContext, outputCollector);
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



