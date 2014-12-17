package com.forter.monitoring;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import com.forter.monitoring.eventSender.EventSender;
import com.forter.monitoring.eventSender.EventsAware;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;


/*
This class creates a monitored wrapper around other spout classes.
The usage is -
MonitoredSpout ms = new MonitoredSpout(new SpoutToMonitor());
*/
public class MonitoredSpout implements IRichSpout {
    private final IRichSpout delegate;
    private transient Logger logger;
    private String spoutService;
    private Optional<String> idName;
    private Monitor monitor;

    public MonitoredSpout(IRichSpout delegate) {
        this.delegate = delegate;
    }

    private static void injectEventSender(IRichSpout delegate, EventSender eventSender) {
        if(delegate instanceof EventsAware) {
            ((EventsAware) delegate).setEventSender(eventSender);
        }
    }

    @Override
    public void open(Map conf, final TopologyContext context, SpoutOutputCollector collector) {
        logger = LoggerFactory.getLogger(delegate.getClass());
        spoutService = context.getThisComponentId();
        monitor = new Monitor(conf, spoutService);
        injectEventSender(delegate, monitor);
        try {
            delegate.open(conf, context, new SpoutOutputCollector(collector) {
                @Override
                public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
                    List<Integer> emitResult = super.emit(streamId, tuple, messageId);
                    monitor.startLatency(messageId);
                    return emitResult;
                }

                @Override
                public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
                    super.emitDirect(taskId, streamId, tuple, messageId);
                    monitor.startLatency(messageId);
                }
            });
        } catch(Throwable t) {
            logger.warn("Error during spout open : ", t);
            throw Throwables.propagate(t);
        }
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void nextTuple() {
        try {
            delegate.nextTuple();
        } catch(Throwable t) {
            logger.info("Error during spout nextTuple : ", t);
            throw Throwables.propagate(t);
        }
    }

    @Override
    public void ack(Object id) {
        if(idName.isPresent()) {
            Map<String, String> attributes = Maps.newHashMap();
            attributes.put(idName.get(), String.valueOf(id));

            monitor.endSpoutLatency(id, spoutService, attributes, null);
        } else {
            monitor.endSpoutLatency(id, spoutService, null, null);
        }

        try {
            delegate.ack(id);
        } catch(Throwable t) {
            logger.info("Error during spout ack : ", t);
            throw Throwables.propagate(t);
        }
    }

    @Override
    public void fail(Object id) {
        if(idName.isPresent()) {
            Map<String, String> attributes = Maps.newHashMap();
            attributes.put(idName.get(), String.valueOf(id));

            monitor.endSpoutLatency(id, spoutService, attributes, new Throwable("Storm failed."));
        } else {
            monitor.endLatency(id, spoutService, null, new Throwable("Storm failed."));
        }
        try {
            delegate.fail(id);
        } catch(Throwable t) {
            logger.info("Error during spout fail : ", t);
            throw Throwables.propagate(t);
        }
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

    /* A function to set the id name in the tuple.
     * The id name is a unique id to send via custom attributes to riemann,
     * which can later be used to filter events in various ways (riemann, kibana, etc).
     * In the spout, the Id value is the id that the spout.ack receives as parameter.
     */
    public void setIdName(String idName) {
        this.idName = Optional.of(idName);
    }
}