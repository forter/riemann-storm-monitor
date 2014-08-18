package com.forter.monitoring;

import com.aphyr.riemann.client.RiemannClient;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/*
* This class represents the connection to riemann.
* It handles the entire connection process.
*/
public class RiemannConnection {
    private static Logger logger = LoggerFactory.getLogger(RiemannConnection.class);

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
        try {
            String machinePrefix = (discover.retrieveName().startsWith("prod") ? "prod" : "develop");
            String ipAddress = (Iterables.get(discover.describeInstancesByName(machinePrefix + "-riemann-instance"), 0)).getPrivateIpAddress();
            return ipAddress;
        } catch (Throwable t) {
            logger.error("Error getting getRiemannIP", t);
            throw Throwables.propagate(t);
        }
    }

    public RiemannClient getClient() {
        return client;
    }

};