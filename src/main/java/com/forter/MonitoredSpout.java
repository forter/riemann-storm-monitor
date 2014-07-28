/*** Created by yaniv on 23/07/14.*/

package com.forter;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import com.forter.Monitor;

public class MonitoredSpout implements IRichSpout {
    public static class MonitoringMessage {
        public Object id;
        public String service;
        public long startTime;

        public MonitoringMessage(Object id, String service, long startTime) {
            this.id = id;
            this.service = service;
            this.startTime = startTime;
        }

    }


    private IRichSpout delegate;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public MonitoredSpout(IRichSpout delegate) {
        this.delegate = delegate;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        //if(!Monitor.connection.client.isConnected())
            //Monitor.connection.connect();

        delegate.open(conf, context, new SpoutOutputCollector(collector) {
            @Override
            public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
                return super.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
            }

            @Override
            public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
                super.emitDirect(taskId, streamId, tuple, newStreamMessageId(streamId, messageId));
            }

            private MonitoringMessage newStreamMessageId(String stream, Object messageId) {
                return new MonitoringMessage(messageId, stream, System.nanoTime());
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
        logger.info("THE LATENCY OF SERVICE " + ((MonitoringMessage)id).id + " IS " +(System.nanoTime() - ((MonitoringMessage)id).startTime) / 1000000);

        delegate.ack(id);
    }

    @Override
    public void fail(Object id) {
        logger.info("THE FAILED LATENCY OF SERVICE " + ((MonitoringMessage)id).id + " IS " +(System.nanoTime() - ((MonitoringMessage)id).startTime) / 1000000);
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