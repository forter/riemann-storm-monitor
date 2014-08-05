package com.forter.monitoring;


public interface IEventSender {
    void sendThroughputEvent(String service, String messageId);
}
