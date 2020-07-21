package com.forter.monitoring.utils;

import com.forter.monitoring.eventSender.EventSender;
import com.forter.monitoring.eventSender.RiemannEventSender;
import com.google.common.base.Throwables;

import java.io.IOException;

public class EventSenderSingleton {
    private final EventSender sender;

    public EventSender getSender() {
        return this.sender;
    }

    private static class SingletonHolder {
        private static final EventSenderSingleton INSTANCE = new EventSenderSingleton();
    }

    public static EventSenderSingleton getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private EventSenderSingleton() {
        RiemannConnection connection = new RiemannConnection();
        try {
            connection.connect(RiemannDiscovery.getInstance().getRiemannHost());
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        this.sender = new RiemannEventSender(connection);
    }
}
