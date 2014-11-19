package com.forter.monitoring.eventSender;

import com.forter.monitoring.events.ExceptionEvent;
import com.forter.monitoring.events.LatencyEvent;
import com.forter.monitoring.events.RiemannEvent;
import com.forter.monitoring.events.ThroughputEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes event to default logger using info level
 */
public class LoggerEventSender implements EventSender {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void send(RiemannEvent event) {
        logger.info("event send Event : {}", event);
    }
}
