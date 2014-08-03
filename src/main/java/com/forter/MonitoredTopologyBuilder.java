package com.forter;

import backtype.storm.topology.*;

/**
 * A {@link backtype.storm.topology.TopologyBuilder} that automatically wraps added bolts with {@link com.forter.MonitoredBolt} and Spouts with {@link com.forter.MonitoredSpout}
 */
public class MonitoredTopologyBuilder extends TopologyBuilder {

    @Override
    public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelism_hint) {
        return super.setSpout(id, new MonitoredSpout(spout), parallelism_hint);
    }

    @Override
    public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelism_hint) {
        return super.setBolt(id, new MonitoredBolt(bolt), parallelism_hint);
    }
}
