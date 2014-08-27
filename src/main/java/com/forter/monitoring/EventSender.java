package com.forter.monitoring;


public interface EventSender {
    void sendThroughputEvent(String service, String messageId);
    void sendException(Throwable t, String service);
    void sendException(String description, String service);
    void sendLatency(long latency, String service, Throwable er);
    void sendEvent(String description, String service, double metric, String ... tags);
}
