package com.forter;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/*
This singleton class centralizes the storm-monitoring functions.
The monitored bolts and spouts will use the functions in this class.
 */
public class Monitor {
    private static volatile transient Monitor singleton;

    private final Map<Object, Long> startTimestampPerId;
    private final RiemannConnection connection;
    private final Logger logger = LoggerFactory.getLogger("MonitorLogger");

    private Monitor() {
        startTimestampPerId = Maps.newConcurrentMap();
        connection = new RiemannConnection();
        connection.connect();
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

    private void sendLatency(long latency, String service, Throwable er) {
        try {
            connection.getClient().event()
                    .description("This is a storm latency.")
                    .metric(latency)
                    .service(service)
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