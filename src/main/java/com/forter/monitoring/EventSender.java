package com.forter.monitoring;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EventSender implements IEventSender {
    private final RiemannConnection connection;
    private final String machineName;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

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
            logger.warn("Riemann error during send : ", t);
        }
    }

    @Override
    public void sendLatency(long latency, String service, Throwable er) {
        try {
            connection.getClient().event()
                    .metric(latency)
                    .service(machineName + " " + service + " latency." )
                    .tags("storm", "latency")
                    .state(er == null ? "success" : "failure").send();
        } catch(Throwable t) {
            logger.warn("Riemann error during send : ", t);
        }
    }

    @Override
    public void sendException(Throwable t, String service) {
        sendException(Throwables.getStackTraceAsString(t), service);
    }

    @Override
    public void sendException(String description, String service) {
        try {
            connection.getClient().event()
                    .description(description)
                    .service(machineName + " " + service)
                    .tags("storm", "uncaught-exception").send();
        } catch (Throwable t) {
            logger.warn("Riemann error during exception ("+description+") send attempt: ", t);
        }
    }

    @Override
    public void sendEvent(String description, String service, double metric) {
        try {
            connection.getClient().event()
                    .description(description)
                    .service(machineName + " " + service)
                    .metric(metric)
                    .tags("storm").send();
        } catch (Throwable t) {
            logger.warn("Riemann error during general event ("+description+") send attempt: ", t);
        }
    }
}