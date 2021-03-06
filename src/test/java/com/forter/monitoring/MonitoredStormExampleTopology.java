package com.forter.monitoring;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forter.monitoring.eventSender.EventSender;
import com.forter.monitoring.eventSender.EventsAware;
import com.forter.monitoring.events.ThroughputEvent;
import com.google.common.base.Throwables;

import java.util.Map;

/*
This class is a testing class.
It defined two inner classes - a mock bolt and a mock spout. this classes are the bases for the monitoring wrappers.
 */
public class MonitoredStormExampleTopology {
    private static ObjectMapper mapper = new ObjectMapper();

    public static class MockSpout extends BaseRichSpout implements EventsAware {
        private SpoutOutputCollector collector;
        private String serv;
        private int lastId = 0;
        private EventSender es;

        @Override
        public void setEventSender(EventSender es) {
            this.es = es;
        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            this.serv = context.getThisComponentId();
        }

        @Override
        public void nextTuple() {
            es.send(new ThroughputEvent().service("nextTuple"));
            ObjectNode metadata = mapper.createObjectNode();

            metadata.put("jobId", String.valueOf(lastId));

            collector.emit(new Values("", String.valueOf(lastId), metadata), lastId++);
        }

        @Override
        public void ack(Object id) {
        }

        @Override
        public void fail(Object id) {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "jobId", "metadata"));
        }
    }

    public static class MockBolt extends BaseBasicBolt implements IBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "jobId", "metadata"));
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

        MonitoredSpout mSpout = new DefaultMonitoredSpout(new MockSpout());
        mSpout.setIdName("jobId");
        MonitoredBolt mBolt = new DefaultMonitoredBolt(new BasicBoltExecutor(new MockBolt()));

        builder.setSpout("testMockSpout", mSpout, 1);
        builder.setBolt("testMockBolt", mBolt, 1).localOrShuffleGrouping("testMockSpout");

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(1);
        conf.put("topology.riemann.attributes", "foo=1,bar=baz");
        conf.put("topology.monitoring.latencies.map.maxSize", 500);
        conf.put("topology.monitoring.latencies.map.maxTimeSeconds", 120);
        conf.put("topology.monitoring.latencies.map.maxConcurrency", (int) 4);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());

        Thread.sleep(6000 * 1000);
        cluster.shutdown();
    }
}