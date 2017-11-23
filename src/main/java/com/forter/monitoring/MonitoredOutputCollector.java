package com.forter.monitoring;

import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.FailedException;
import org.apache.storm.tuple.Tuple;
import com.forter.monitoring.events.ExceptionEvent;
import com.forter.monitoring.utils.PairKey;

import java.util.Collection;
import java.util.List;

public class MonitoredOutputCollector extends OutputCollector {
    private final Monitor monitor;
    private final MonitoredBolt monitoredBolt;
    private final int latencyFraction;
    private final IOutputCollector delegate;

    MonitoredOutputCollector(MonitoredBolt monitoredBolt, IOutputCollector delegate, int latencyFraction) {
        super(delegate);
        this.monitoredBolt = monitoredBolt;
        this.monitor = monitoredBolt.getMonitor();
        this.latencyFraction = latencyFraction;
        this.delegate = delegate;
    }

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        if (anchors != null) {
            for (Tuple t : anchors) {
                if (shouldMonitorFraction(t)) {
                    monitor.startLatency(pair(t), LatencyType.EMIT);
                }
            }
        }

        try {
            return super.emit(streamId, anchors, tuple);
        } finally {
            if (anchors != null) {
                for (Tuple t : anchors) {
                    if (shouldMonitorFraction(t)) {
                        monitor.endLatency(pair(t), LatencyType.EMIT);
                    }
                }
            }
        }
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        super.emitDirect(taskId, streamId, anchors, tuple);
    }

    @Override
    public void ack(Tuple input) {
        if (shouldMonitorFraction(input) && monitor.shouldMonitor(input)) {
            if (shouldIgnore(input, true)) {
                monitor.ignoreExecute(pair(input));
            } else {
                monitor.endExecute(pair(input), getCustomAttributes(input), true);
            }
        }
        super.ack(input);
    }

    @Override
    public void fail(Tuple input) {
        if (shouldMonitorFraction(input) && monitor.shouldMonitor(input)) {
            if (shouldIgnore(input, false)) {
                monitor.ignoreExecute(pair(input));
            } else {
                monitor.endExecute(pair(input), getCustomAttributes(input), false);
            }
        }
        super.fail(input);
    }

    @Override
    public void reportError(Throwable error) {
        Throwable t = error;

        if (t instanceof FailedException) {
            while (t instanceof FailedException && t.getCause() != null) {
                t = t.getCause();
            }
        }

        monitor.send(new ExceptionEvent(t).service(this.monitoredBolt.componentId));
        super.reportError(t);
    }

    private boolean shouldIgnore(Tuple input, boolean isAck) {
        LatencyIgnoreToggle latencyIgnoreToggle = this.monitoredBolt.getLatencyIgnoreToggle();
        return latencyIgnoreToggle != null && latencyIgnoreToggle.shouldIgnoreLatency(input, isAck);
    }

    private EventProperties getCustomAttributes(Tuple input) {
        CustomLatencyAttributesGenerator customAttributesGen =
                this.monitoredBolt.getCustomLatencyAttributesGenerator();
        if (customAttributesGen != null) {
            return customAttributesGen.getCustomAttributes(input);
        }
        return null;
    }

    private boolean shouldMonitorFraction(Tuple t) {
        return t.hashCode() % latencyFraction == 0;
    }

    private PairKey pair(Tuple tuple) {
        return new PairKey(this.monitoredBolt, tuple);
    }

    public IOutputCollector getDelegate() {
        return delegate;
    }
}
