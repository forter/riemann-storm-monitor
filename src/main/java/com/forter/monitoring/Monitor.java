package com.forter.monitoring;
import com.forter.monitoring.eventSender.EventSender;
import com.forter.monitoring.eventSender.LoggerEventSender;
import com.forter.monitoring.events.ExceptionEvent;
import com.forter.monitoring.events.LatencyEvent;
import com.forter.monitoring.utils.RiemannDiscovery;
import com.forter.monitoring.eventSender.RiemannEventSender;
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

    public void endLatency(Object latencyId, String service, Map<String,String> customAttributes, Throwable er) {
        endLatency(latencyId, service, customAttributes, null, null, er);
    }

    public void endLatency(Object latencyId, String service, Map<String,String> customAttributes, String stormIdName, String stormIdValue, Throwable er) {
        if(startTimestampPerId.containsKey(latencyId)) {
            long elapsed = NANOSECONDS.toMillis(System.nanoTime() - startTimestampPerId.get(latencyId));

            LatencyEvent event = new LatencyEvent(elapsed)
                    .service(service)
                    .error(er);

            if(stormIdName != null && stormIdValue != null) {
                event.attribute(stormIdName, stormIdValue);
            }

            event.attributes(customAttributes);

            eventSender.send(event);

            startTimestampPerId.remove(latencyId);
            if (logger.isDebugEnabled()) {
                logger.debug("Monitored latency {} for key {}", elapsed, latencyId);
            }
        } else {
            eventSender.send(new ExceptionEvent("Latency monitor doesn't recognize key.")
                    .service(service)
                    .attributes(customAttributes));
            if (er == null) {
                logger.warn("Latency monitor doesn't recognize key {}.", latencyId);
            }
            else {
                eventSender.send(new ExceptionEvent(er)
                        .service(service)
                        .attributes(customAttributes));
                logger.warn("Latency monitor doesn't recognize key {}. Swallowed exception {}", latencyId, er);
            }
        }
    }
}