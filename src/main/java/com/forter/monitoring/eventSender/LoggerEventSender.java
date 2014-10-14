package com.forter.monitoring.eventSender;

import com.forter.monitoring.eventSender.EventSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Writes event to default logger using info level
 */
public class LoggerEventSender implements EventSender {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void sendThroughputEvent(String service, String messageId) {
        logger.info("event sendThroughputEvent({},{})", service, messageId);
    }

    @Override
    public void sendException(Throwable t, String service) {
        logger.info("event sendException({},{})", String.valueOf(t), service);
    }

    @Override
    public void sendException(String description, String service) {
        logger.info("event sendException({},{})", description, service);
    }

    @Override
    public void sendLatency(long latency, String service, Throwable t) {
        logger.info("event sendLatency({},{},{})", latency, service, String.valueOf(t));
    }

    @Override
    public void sendEvent(String description, String service, double metric, String... tags) {
        logger.info("event sendEvent({},{},{},{})", description, service, metric, Arrays.toString(tags));
    }
}