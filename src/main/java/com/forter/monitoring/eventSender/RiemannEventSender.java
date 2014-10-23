package com.forter.monitoring.eventSender;
import com.forter.monitoring.events.ExceptionEvent;
import com.forter.monitoring.events.LatencyEvent;
import com.forter.monitoring.events.RiemannEvent;
import com.forter.monitoring.events.ThroughputEvent;
import com.forter.monitoring.utils.RiemannConnection;

import com.aphyr.riemann.client.RiemannClient;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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
    }

    public void sendRawWithAck(RiemannEvent event) {
        Boolean success = null;
        try {
            success = createEvent()
                    .description(event.description)
                    .host(event.host)
                    .service(event.service)
                    .state(event.state)
                    .time(event.time)
                    .metric(event.metric)
                    .ttl(event.ttl)
                    .tags(event.tags)
                    .attributes(event.customAttributes)
                    .sendWithAck();
            if (!Boolean.TRUE.equals(success)) {
                throw new IOException("No ACK received from riemann.");
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public RiemannClient getRiemannClient() {
        return connection.getClient();
    }
}