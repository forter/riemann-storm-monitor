package com.forter.monitoring.utils;

import com.aphyr.riemann.client.RiemannClient;
import com.forter.monitoring.utils.RiemannDiscovery;
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
    private final String machineName;
    private String riemannIP;
    private RiemannClient client;
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private final int riemannPort = 5555;
    public RiemannConnection(String machineName) {
        this.machineName = machineName;
    }

    public enum RiemannType {
        DEFAULT,
        JS
    }

    public void connect() {
        this.connect(RiemannType.DEFAULT);
    }

    public void connect(RiemannType requested_type) {
        if (client == null) {
            try {
                riemannIP = getRiemannIP(requested_type);
                client = RiemannClient.tcp(riemannIP, riemannPort);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            try {
                // initializes client, connection is actually async
                client.connect();
            }
            catch (IOException e) {
                logger.info("Failed connecting to riemann " + riemannIP + ":" + riemannPort + " riemann-java-client will try reconnecting every 5 seconds.");
            }
        }
    }


    private String getRiemannIP() throws IOException {
        return this.getRiemannIP(RiemannType.DEFAULT);
    }

    private String getRiemannIP(RiemannType riemann_type) throws IOException {
        final String riemannMachineName;
        final boolean isProd = machineName.startsWith("prod");
        final boolean isProdVT = machineName.startsWith("prod-vt");

        if (isProd) {
            if (riemann_type == RiemannType.JS) {
                riemannMachineName = "prod-riemannjs-instance";
            }
            else if (isProdVT) {
                riemannMachineName = "prod-vtriemann-instance";
            }
            else {
                riemannMachineName = "prod-riemann-instance";
            }
        }
        else {
            riemannMachineName = "develop-riemann-instance";
        }
        return (Iterables.get(RiemannDiscovery.getInstance().describeInstancesByName(riemannMachineName), 0)).getPrivateIpAddress();
    }

    public RiemannClient getClient() {
        return client;
    }

};