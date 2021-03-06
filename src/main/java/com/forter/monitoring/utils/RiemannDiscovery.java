package com.forter.monitoring.utils;

import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Optional.of;

/*
This class represents the discovery of the riemann machine.
It is possible to use it to get the IP of a machine, based on its name / id.
*/
public class RiemannDiscovery {
    private final AmazonEC2 ec2Client;
    private final Object nameCacheLocker = new Object();
    private Optional<String> retrievedName = Optional.absent();

    private static class SingletonHolder {
        private static final RiemannDiscovery INSTANCE = new RiemannDiscovery();
    }

    private RiemannDiscovery() {
        ec2Client = new AmazonEC2Client(new AWSCredentialsProviderChain(new InstanceProfileCredentialsProvider(), new EnvironmentVariableCredentialsProvider()));
    }

    public String getRiemannHost() throws IOException {
        Optional<String> machineNameOpt = retrieveName();
        Preconditions.checkArgument(machineNameOpt.isPresent());
        String machineName = machineNameOpt.get();

        final String riemannMachineName;
        if (machineName.startsWith("prod-vt")) {
            riemannMachineName = "prod-vtriemann-instance";
        } else if (machineName.startsWith("prod")) {
            riemannMachineName = "prod-riemann-instance";
        } else {
            riemannMachineName = "develop-riemann-instance";
        }

        return (Iterables.get(RiemannDiscovery.getInstance().describeInstancesByName(riemannMachineName), 0)).getPrivateIpAddress();
    }

    public static RiemannDiscovery getInstance() {
        return SingletonHolder.INSTANCE;
    }

    public String retrieveInstanceId() throws IOException {
        return retrieveMetadata("instance-id");
    }

    public String retrieveMetadata(String metadata) throws IOException {
        StringBuilder result = new StringBuilder();
        String inputLine;
        URL url = new URL("http://instance-data/latest/meta-data/" + metadata);
        URLConnection connection = url.openConnection();

        try (BufferedReader in = new BufferedReader(new InputStreamReader(
            connection.getInputStream()))) {
            while ((inputLine = in.readLine()) != null) {
                result.append(inputLine);
            }
        }
        return result.toString();
    }

    public boolean isAWS() {
        String path = System.getProperty("AWS_CLI_CONFIG_FILE", System.getProperty("user.home") + "/.aws/config");
        File f = new File(path);
        return !f.exists();
    }

    public Optional<String> retrieveName() throws IOException {
        if (retrievedName.isPresent()) return retrievedName;
        synchronized (nameCacheLocker) {
            if (retrievedName.isPresent()) return retrievedName;
            if (!isAWS()) {
                retrievedName = Optional.absent();
            } else {
                final String instanceId = retrieveInstanceId();
                final Instance instance = describeInstanceById(instanceId);
                retrievedName = of(getInstanceName(instance));
            }
        }
        return retrievedName;
    }

    private String getInstanceName(Instance instance) {
        return Iterables.getOnlyElement(Iterables.filter(instance.getTags(),
                new Predicate<Tag>() {
                    public boolean apply(Tag tag) {
                        return tag.getKey().equalsIgnoreCase("name");
                    }
                }
        )).getValue();
    }

    public Instance describeInstanceById(String instanceId) {
        final DescribeInstancesRequest request = new DescribeInstancesRequest();
        request.setInstanceIds(Collections.singletonList(instanceId));
        return Iterables.getOnlyElement(describeInstances(request));
    }

    public Iterable<Instance> describeInstancesByName(final String name) {
        final Filter runningFilter = new Filter().withName("instance-state-name").withValues("running");
        Filter nameFilter = new Filter().withName("tag:Name").withValues(name);
        return describeInstances(
                new DescribeInstancesRequest().withFilters(nameFilter,runningFilter));
    }

    private Iterable<Instance> describeInstances(DescribeInstancesRequest request) {
        final DescribeInstancesResult result = ec2Client.describeInstances(request);
        return Iterables.concat(
                Iterables.transform(result.getReservations(),
                        new Function<Reservation, List<Instance>>() {
                            @Override
                            public List<Instance> apply(Reservation reservation) {
                                return reservation.getInstances();
                            }
                        }));
    }
}
