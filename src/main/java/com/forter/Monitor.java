package com.forter;

import com.google.common.collect.Maps;

import java.util.Map;

/*** Created by yaniv on 27/07/14.*/

public class Monitor {
    private static final Map<Integer,Long> startTimestampPerId = Maps.newHashMap();
    public static final RiemannConnection connection = new RiemannConnection();

    private static void sendLatency(long latency, String service, RuntimeException er) {
        connection.client.event().metric(latency).service(service).tags("storm", "latency").state((latency > 3000 || er != null) ? "BAD" : "GOOD").send();
    }

    public static void startLatency(Object messageId) {
        startTimestampPerId.put((Integer)messageId, System.nanoTime());
    }

    public static void endLatency(Object id, String service, RuntimeException er) {
        long elapsed = (System.nanoTime() - startTimestampPerId.get(id)) / 1000000;
        sendLatency(elapsed, service, er);
    }


}