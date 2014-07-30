package com.forter;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.google.common.base.Throwables;

import java.util.Map;


/*
This class creates a monitored wrapper around other bolt classes.
The usage is -
MonitoredBolt mb = new MonitoredBolt(new BoltToMonitor());
*/
public class MonitoredBolt implements IBasicBolt {
    private IBasicBolt delegate;
    private String boltService;
    private Monitor monitor;

    public MonitoredBolt(IBasicBolt delegate) {
        this.monitor = Monitor.getMonitor();
        this.delegate = delegate;
    }

    @Override
    public void prepare(Map conf, TopologyContext context) {
        boltService = context.getThisComponentId();
        delegate.prepare(conf, context);
    }

    @Override
    public void execute(Tuple tuple, final BasicOutputCollector collector) {
        Throwable er = null;
        monitor.startLatency(tuple.getMessageId());
        try {
            delegate.execute(tuple, collector);
        }
        catch(Throwable error) {
            er = error;
            throw Throwables.propagate(error);
        }
        finally {
            monitor.endLatency(tuple.getMessageId(), this.boltService, er);
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



