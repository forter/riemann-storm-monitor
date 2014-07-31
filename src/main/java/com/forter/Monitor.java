package com.forter;
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
    private static volatile transient Monitor singleMonitor;

    private final IEventSender eventSender;
    private final Map<Object, Long> startTimestampPerId;
    private final RiemannConnection connection;
    private final String machineName;
    private final Logger logger = LoggerFactory.getLogger("MonitorLogger");


    private Monitor() {
        machineName = getMachineName();
        eventSender = new IEventSender() {
            @Override
            public void sendThroughputEvent(String service, String messageId) {
                try {
                    connection.getClient().event()
                            .metric(1)
                            .service(machineName + ": throughput :" + service)
                            .tags("storm", "throughput").send();
                } catch (Throwable t) {
                    logger.warn("Riemann error during send : " + t.getStackTrace());
                }
            }
        };
        startTimestampPerId = Maps.newConcurrentMap();
        connection = new RiemannConnection();
        connection.connect();
    }

    public static Monitor getMonitor() {
        if(singleMonitor == null) {
            synchronized (Monitor.class) {
                if(singleMonitor == null)
                    singleMonitor = new Monitor();
            }
        }
        return singleMonitor;
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

    private void sendLatency(long latency, String service, Throwable er) {
        try {
            connection.getClient().event()
                    .metric(latency)
                    .service(machineName + ": latency :" + service)
                    .tags("storm", "latency")
                    .state(er == null ? "success" : "failure").send();
        } catch(Throwable t) {
            logger.warn("Riemann error during send : " + t.getStackTrace());
        }
    }

    public void startLatency(Object messageId) {
        startTimestampPerId.put(messageId, System.nanoTime());
    }

    public void endLatency(Object id, String service, Throwable er) {
        if(startTimestampPerId.containsKey(id)) {
            long elapsed = NANOSECONDS.toMillis(System.nanoTime() - startTimestampPerId.get(id));
            sendLatency(elapsed, service, er);
            startTimestampPerId.remove(id);
        }
        else {
            try {
                connection.getClient().event().description("Storm Latency Hash-Map doesn't contain received key.").service(service).tags("uncaught-exception");
            } catch (Throwable t) {
                logger.warn("Riemann error during send : " + t.getStackTrace());
            }
        }
    }
}