package com.forter;

import com.google.common.collect.Maps;

import java.util.Map;

/*** Created by yaniv on 27/07/14.*/

public class Monitor {


    public static final Map<Integer,Long> startTimestampPerId = Maps.newHashMap();
    public static final RiemannConnection connection = new RiemannConnection();



    private static void sendLatency(long latency, String service, RuntimeException er) {
        connection.client.event().metric(latency).service(service).tags("storm", "latency").state((latency > 3000 || er != null) ? "BAD" : "GOOD").send();
    }
}