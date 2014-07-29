package com.forter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.*;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;


public class MonitoredStormExampleTopology {

    public static class MockSpout extends BaseRichSpout {
        private SpoutOutputCollector _collector;
        private int lastId = 0;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void nextTuple() {
            _collector.emit(new Values(""), lastId++);
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

    public static class MockBolt extends BaseRichBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        }

        @Override
        public void execute(Tuple tuple) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("ItaiSpout",new MonitoredSpout(new MockSpout()), 1);
        builder.setBolt("myBolt", new MockBolt(), 1).localOrShuffleGrouping("ItaiSpout");

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());

        Thread.sleep(6000 * 1000);
        cluster.shutdown();
    }
}