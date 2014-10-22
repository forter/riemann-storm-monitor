package com.forter.monitoring.eventSender;


import com.forter.monitoring.events.ExceptionEvent;
import com.forter.monitoring.events.LatencyEvent;
import com.forter.monitoring.events.RiemannEvent;
import com.forter.monitoring.events.ThroughputEvent;

public interface EventSender {
    void send(ThroughputEvent event);
    void send(ExceptionEvent event);
    void send(LatencyEvent event);
    void send(RiemannEvent event);
}
