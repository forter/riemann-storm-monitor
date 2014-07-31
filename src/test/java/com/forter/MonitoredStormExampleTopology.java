package com.forter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.*;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.base.Throwables;

import java.util.Map;

/*
This class is a testing class.
It defined two inner classes - a mock bolt and a mock spout. this classes are the bases for the monitoring wrappers.
 */
public class MonitoredStormExampleTopology {

    public static class MockSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;
        private int lastId = 0;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            collector.emit(new Values(""), lastId++);
        }

        @Override
        public void ack(Object id) {
        }

        @Override
        public void fail(Object id) {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

    }

    public static class MockBolt extends BaseBasicBolt implements IBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",new MonitoredSpout(new MockSpout()), 1);
        builder.setBolt("bolt", new MonitoredBolt(new MockBolt()), 1).localOrShuffleGrouping("spout");

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());

        Thread.sleep(6000 * 1000);
        cluster.shutdown();
    }
}