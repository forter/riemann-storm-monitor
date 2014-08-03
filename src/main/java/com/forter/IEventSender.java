package com.forter;


public interface IEventSender {
    void sendThroughputEvent(String service, String messageId);
}
