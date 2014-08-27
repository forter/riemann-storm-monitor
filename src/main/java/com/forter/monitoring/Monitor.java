package com.forter.monitoring;
import com.google.common.base.Optional;
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
    private final Logger logger = LoggerFactory.getLogger(this.getClass());


    private Monitor() {
        Optional<String> machineName = getMachineName();
        startTimestampPerId = Maps.newConcurrentMap();
        if (machineName.isPresent()) {
            eventSender = new RiemannEventSender(machineName.get());
        } else {
            //fallback for local mode
            eventSender = new LoggerEventSender();
        }
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

    public EventSender getEventSender() {
        return eventSender;
    }

    private Optional<String> getMachineName() {
        try {
            return new RiemannDiscovery().retrieveName();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public void startLatency(Object id) {
        final long now = System.nanoTime();

        startTimestampPerId.put(id, now);

        if (logger.isDebugEnabled()) {
            logger.debug("Monitoring latency for key {}", id);
        }
    }

    public void endLatency(Object id, String service, Throwable er) {
        if(startTimestampPerId.containsKey(id)) {
            long elapsed = NANOSECONDS.toMillis(System.nanoTime() - startTimestampPerId.get(id));
            eventSender.sendLatency(elapsed, service, er);
            startTimestampPerId.remove(id);
            if (logger.isDebugEnabled()) {
                logger.debug("Monitored latency {} for key {}", elapsed, id);
            }
        }
        else {
            eventSender.sendException("Latency monitor doesn't recognize key.", service);
            if (er == null) {
                logger.warn("Latency monitor doesn't recognize key {}.", id);
            }
            else {
                eventSender.sendException(er, service);
                logger.warn("Latency monitor doesn't recognize key {}. Swallowed exception {}", id, er);
            }
        }
    }
}