package com.forter;
import com.google.common.collect.Maps;
import java.util.Map;

/*
This singleton class centralizes the storm-monitoring functions.
The monitored bolts and spouts will use the functions in this class.
 */
public class Monitor {
    private static transient Monitor singleton;

    private final Map<Object, Long> startTimestampPerId;
    private final RiemannConnection connection;

    private Monitor() {
        startTimestampPerId = Maps.newConcurrentMap();
        connection = new RiemannConnection();
        connection.connect();
    }

    public synchronized static Monitor getMonitor() {
        if(singleton == null) {
            singleton = new Monitor();
        }
        return singleton;
    }

    private void sendLatency(long latency, String service, Throwable er) {
        connection.getClient().event()
                .description("This is a storm latency.")
                .metric(latency)
                .service(service)
                .tags("storm", "latency")
                .state(er == null ? "success" : "failure").send();
    }

    public void startLatency(Object messageId) {
        startTimestampPerId.put(messageId, System.nanoTime());
    }

    public void endLatency(Object id, String service, Throwable er) {
        if(startTimestampPerId.containsKey(id)) {
            long elapsed = (System.nanoTime() - startTimestampPerId.get(id)) / 1000000;
            sendLatency(elapsed, service, er);
        }
    }


}