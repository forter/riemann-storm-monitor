package com.forter.monitoring.eventSender;
import com.forter.monitoring.events.ExceptionEvent;
import com.forter.monitoring.events.LatencyEvent;
import com.forter.monitoring.events.ThroughputEvent;
import com.forter.monitoring.utils.RiemannConnection;
import com.forter.monitoring.events.RiemannEvent;
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

    private com.aphyr.riemann.client.EventDSL createEvent() {
        return connection.getClient().event();
    }

    @Override
    public void send(ThroughputEvent event) {
        try {
            send((RiemannEvent) event);
        } catch(Throwable t) {
            logger.warn("Riemann error during throughput event ("+ event.description+") send attempt: ", t);
        }
    }

    @Override
    public void send(ExceptionEvent event) {
        try {
            send((RiemannEvent) event);
        } catch(Throwable t) {
            logger.warn("Riemann error during exception event ("+ event.description+") send attempt: ", t);
        }
    }

    @Override
    public void send(LatencyEvent event) {
        try {
            send((RiemannEvent) event);
        } catch(Throwable t) {
            logger.warn("Riemann error during latency event ("+ event.description+") send attempt: ", t);
        }
    }

    @Override
    public void send(RiemannEvent event) {
        try {
            createEvent()
                    .description(event.description)
                    .host(event.host)
                    .service(machineName + " " + event.service)
                    .state(event.state)
                    .time(event.time)
                    .metric(event.metric)
                    .ttl(event.ttl)
                    .tag("storm")
                    .tags(event.tags)
                    .attributes(event.customAttributes)
                    .send();

        } catch (Throwable t) {
            logger.warn("Riemann error during event ("+ event.description+") send attempt: ", t);
        }
    }

    public void sendRaw(RiemannEvent event) {
        try {
            createEvent()
                    .description(event.description)
                    .host(event.host)
                    .service(event.service)
                    .state(event.state)
                    .time(event.time)
                    .metric(event.metric)
                    .ttl(event.ttl)
                    .tags(event.tags)
                    .attributes(event.customAttributes)
                    .send();
        } catch (Throwable t) {
            logger.warn("Riemann error during event ("+event.description+") send attempt: ", t);
        }
    }
}