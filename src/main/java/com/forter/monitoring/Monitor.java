package com.forter.monitoring;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Map;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/*
This singleton class centralizes the storm-monitoring functions.
The monitored bolts and spouts will use the functions in this class.
 */
public class Monitor {
    private static volatile transient Monitor singleton;

    private final EventSender eventSender;
    private final Map<Object, Long> startTimestampPerId;
    private final String machineName;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());


    private Monitor() {
        machineName = getMachineName();
        startTimestampPerId = Maps.newConcurrentMap();;
        eventSender = new EventSender(machineName);
    }

    public static Monitor getMonitor() {
        if(singleton == null) {
            synchronized (Monitor.class) {
                if(singleton == null)
                    singleton = new Monitor();
            }
        }
        return singleton;
    }

    public IEventSender getEventSender() {
        return eventSender;
    }

    private String getMachineName() {
        try {
            return new RiemannDiscovery().retrieveName();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }


    public void startLatency(Object messageId) {
        final long now = System.nanoTime();
        startTimestampPerId.put(messageId, now);
        if (logger.isDebugEnabled()) {
            logger.debug("Added to Hash-Map messageId %s at timestamp %s", messageId, now);
        }
    }

    public void endLatency(Object id, String service, Throwable er) {
        if(startTimestampPerId.containsKey(id)) {
            long elapsed = NANOSECONDS.toMillis(System.nanoTime() - startTimestampPerId.get(id));
            eventSender.sendLatency(elapsed, service, er);
            startTimestampPerId.remove(id);
        }
        else {
            logger.warn("Storm Latency Hash-Map doesn't contain received id %s. Swallowed exception %s", id, er);
            eventSender.sendException("Storm Latency Hash-Map doesn't contain received id.", service);
        }
    }
}