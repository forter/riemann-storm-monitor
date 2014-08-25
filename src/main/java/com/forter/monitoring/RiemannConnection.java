package com.forter.monitoring;

import com.aphyr.riemann.client.RiemannClient;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;

import java.io.IOException;

/*
* This class represents the connection to riemann.
* It handles the entire connection process.
*/
public class RiemannConnection {
    private String riemannIP;
    private RiemannClient client;

    public void connect() {
        if (client == null || !client.isConnected()) {
            try {
                riemannIP = getRiemannIP(new RiemannDiscovery());
                client = RiemannClient.tcp(riemannIP, 5555);
                client.connect();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private String getRiemannIP(RiemannDiscovery discover) throws IOException {
        Optional<String> machineName = discover.retrieveName();
        String machinePrefix = (machineName.isPresent() && machineName.get().startsWith("prod") ? "prod" : "develop");
        return (Iterables.get(discover.describeInstancesByName(machinePrefix + "-riemann-instance"), 0)).getPrivateIpAddress();
    }

    public RiemannClient getClient() {
        return client;
    }

};