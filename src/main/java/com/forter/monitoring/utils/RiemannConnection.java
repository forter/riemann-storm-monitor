package com.forter.monitoring.utils;

import com.aphyr.riemann.client.RiemannClient;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/*
 * This class represents the connection to riemann.
 * It handles the entire connection process.
 */
public class RiemannConnection {
    private String riemannIP;
    private RiemannClient client;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final int riemannPort = 5555;

    public void connect(String ip) {
        if (client == null) {
            try {
                riemannIP = ip;
                client = RiemannClient.tcp(riemannIP, riemannPort);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
            try {
                // initializes client, connection is actually async
                client.connect();
            } catch (IOException e) {
                logger.info("Failed connecting to riemann " + riemannIP + ":" + riemannPort + " riemann-java-client will try reconnecting every 5 seconds.");
            }
        }
    }

    public RiemannClient getClient() {
        return client;
    }
}
