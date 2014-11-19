package com.forter.monitoring.eventSender;
import com.aphyr.riemann.client.EventDSL;
import com.forter.monitoring.events.RiemannEvent;
import com.forter.monitoring.utils.RiemannConnection;

import com.aphyr.riemann.client.RiemannClient;
import com.forter.monitoring.utils.RiemannDiscovery;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RiemannEventSender implements EventSender {
    private static volatile transient RiemannEventSender singleton;
    private final RiemannConnection connection;
    private final String machineName;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    // A temporary field for the v0.8.6.1 fix. will be removed later.
    private final float DEFAULT_TTL_SEC = 5f;

    public static RiemannEventSender getRiemannEventsSender() {
        if(singleton == null) {
            synchronized (RiemannEventSender.class) {
                if(singleton == null) {
                    singleton = new RiemannEventSender();
                }
            }
        }
        return singleton;
    }

    private RiemannEventSender() {
        this.machineName = retrieveMachineName();
        this.connection = new RiemannConnection(machineName);
        connection.connect();
    }

    private String retrieveMachineName() {
        try {
            Optional<String> machineName = new RiemannDiscovery().retrieveName();
            if (!machineName.isPresent()) {
                throw new Error("No machine name!");
            }
            return machineName.get();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }



    private com.aphyr.riemann.client.EventDSL createEvent() {
        return connection.getClient().event();
    }

    @Override
    public void send(RiemannEvent event) {
        try {
            EventDSL eventDSL = createEvent()
                                .description(event.description)
                                .service(machineName + " " + event.service)
                                .state(event.state)
                                .time(System.currentTimeMillis() / 1000L)
                                .metric(event.metric)
                                .ttl(event.ttl == null ? DEFAULT_TTL_SEC : event.ttl)
                                .tag("storm")
                                .tags(event.tags)
                                .attributes(event.customAttributes);

            //To avoid 127.0.0.1 appearing as event host
            if(event.host != null) {
                eventDSL.host(event.host);
            }

            eventDSL.send();
            logger.debug("Event sent - {}", event);

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
                .ttl(event.ttl == null ? DEFAULT_TTL_SEC : event.ttl)
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
                    .ttl(event.ttl == null ? DEFAULT_TTL_SEC : event.ttl)
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