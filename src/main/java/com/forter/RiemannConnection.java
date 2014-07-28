package com.forter;

import com.aphyr.riemann.client.RiemannClient;
import com.google.common.collect.Iterables;

import java.io.IOException;

public class RiemannConnection {
    private String riemannIP;
    public RiemannClient client;

    public void connect() {
        RiemannDiscovery discover = new RiemannDiscovery();
        try {
            String machinePrefix = (discover.retrieveName().startsWith("prod") ? "prod" : "develop");
            //riemannIP = (Iterables.get(discover.describeInstancesByName(machinePrefix + "-riemann-instance"), 0)).getPrivateIpAddress();
            riemannIP = "127.0.0.1";
            client = RiemannClient.tcp(riemannIP, 5555);
            client.connect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void disconnect() {
        try {
            client.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
};