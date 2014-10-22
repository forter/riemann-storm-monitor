package com.forter.monitoring.eventSender;
import com.forter.monitoring.utils.RiemannConnection;
import com.google.common.base.Throwables;
import com.google.common.collect.ObjectArrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RiemannEventSender implements EventSender {
    private final RiemannConnection connection;
    private final String machineName;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public RiemannEventSender(String machineName) {
        this.connection = new RiemannConnection(machineName);
        connection.connect();
        this.machineName = machineName;
    }

    public com.aphyr.riemann.client.EventDSL createEvent() {
        return connection.getClient().event();
    }

    @Override
    public void sendThroughputEvent(String service, String messageId) {
        try {
            createEvent()
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
            createEvent()
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
            createEvent()
                    .description(description)
                    .service(machineName + " " + service)
                    .tags("storm", "uncaught-exception").send();
        } catch (Throwable t) {
            logger.warn("Riemann error during exception ("+description+") send attempt: ", t);
        }
    }

    @Override
    public void sendEvent(String description, String service, double metric, String ... tags) {
        try {
            String tagsArr[] = ObjectArrays.concat(tags, new String[]{"storm"}, String.class);
            createEvent()
                    .description(description)
                    .service(machineName + " " + service)
                    .metric(metric)
                    .tags(tagsArr).send();
        } catch (Throwable t) {
            logger.warn("Riemann error during general event ("+description+") send attempt: ", t);
        }
    }
}