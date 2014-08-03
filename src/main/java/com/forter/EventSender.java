package com.forter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EventSender implements IEventSender {
    private final RiemannConnection connection;
    private final String machineName;
    private final Logger logger = LoggerFactory.getLogger("EventSender");

    public EventSender(String machineName) {
        this.connection = new RiemannConnection();
        connection.connect();
        this.machineName = machineName;
    }

    @Override
    public void sendThroughputEvent(String service, String messageId) {
        try {
            connection.getClient().event()
                    .metric(1)
                    .service(machineName + " " + service + " throughput.")
                    .tags("storm", "throughput").send();
        } catch (Throwable t) {
            logger.warn("Riemann error during send : " + stackTraceToString(t));
        }
    }

    public void sendLatency(long latency, String service, Throwable er) {
        try {
            connection.getClient().event()
                    .metric(latency)
                    .service(machineName + " " + service + " latency." )
                    .tags("storm", "latency")
                    .state(er == null ? "success" : "failure").send();
        } catch(Throwable t) {
            logger.warn("Riemann error during send : " + stackTraceToString(t));
        }
    }

    public void sendException(String description, String service) {
        try {
            connection.getClient().event().description(description).service(service).tags("uncaught-exception").send();
        } catch (Throwable t) {
            logger.warn("Riemann error during exception send : " + stackTraceToString(t));
        }
    }

    private String stackTraceToString(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for(StackTraceElement element : t.getStackTrace())
            sb.append(element.toString() + '\n');
        return sb.toString();
    }
}