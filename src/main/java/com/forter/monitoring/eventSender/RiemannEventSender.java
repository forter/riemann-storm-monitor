package com.forter.monitoring.eventSender;
import com.aphyr.riemann.client.EventDSL;
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

    private com.aphyr.riemann.client.EventDSL createRawEvent(RiemannEvent event) {
        EventDSL eventDSL = connection.getClient().event()
                .description(event.description)
                .host(event.host)
                .service(event.service)
                .state(event.state)
                .tags(event.tags)
                .attributes(event.customAttributes);

        //Giving a null value for all the numeric fields
        if(event.time != null) {
            eventDSL.time(event.time);
        }
        if(event.ttl != null) {
            eventDSL.time(event.ttl);
        }
        if(event.metric != null) {
            eventDSL.time(event.metric);
        }
        return eventDSL;
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
            EventDSL eventDSL = createRawEvent(event)
                                .service(machineName + " " + event.service)
                                .time(System.currentTimeMillis() / 1000L)
                                .tag("storm");

            //To avoid 127.0.0.1 appearing as event host
            if(event.host != null) {
                eventDSL.host(event.host);
            }

            eventDSL.send();

        } catch (Throwable t) {
            logger.warn("Riemann error during event ("+ event.description+") send attempt: ", t);
        }
    }

    public void sendRaw(RiemannEvent event) {
        createRawEvent(event).send();
    }


    public void sendRawWithAck(RiemannEvent event) {
        Boolean success = null;
        try {
            success = createRawEvent(event).sendWithAck();
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