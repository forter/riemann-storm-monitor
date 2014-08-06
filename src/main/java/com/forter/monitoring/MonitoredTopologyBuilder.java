package com.forter.monitoring;

import backtype.storm.topology.*;

/**
 * A {@link backtype.storm.topology.TopologyBuilder} that automatically wraps added bolts with {@link MonitoredBolt} and Spouts with {@link MonitoredSpout}
 */
public class MonitoredTopologyBuilder extends TopologyBuilder {

    @Override
    public BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelism_hint) {
        final MonitoredBolt monitoredBolt = new MonitoredBolt(bolt);
        return setBolt(id, monitoredBolt, parallelism_hint);
    }

    @Override
    public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelism_hint) {
        final MonitoredSpout monitoredSpout = (spout instanceof MonitoredSpout) ? (MonitoredSpout)spout: new MonitoredSpout(spout);
        return super.setSpout(id, monitoredSpout, parallelism_hint);
    }

    @Override
    public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelism_hint) {
        final MonitoredBolt monitoredBolt = (bolt instanceof MonitoredBolt) ? (MonitoredBolt)bolt : new MonitoredBolt(bolt);
        return super.setBolt(id, monitoredBolt, parallelism_hint);
    }
}
