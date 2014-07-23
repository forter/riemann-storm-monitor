import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.regex.Pattern;
import static java.util.Arrays.asList;

public class RiemannDiscovery {
    private final AmazonEC2 ec2Client;

    public RiemannDiscovery() {
        ec2Client = new AmazonEC2Client(new InstanceProfileCredentialsProvider());
    }

    public static String retrieveInstanceId() throws IOException {
        return retrieveMetadata("instance-id");
    }

    public static String retrieveMetadata(String metadata) throws IOException {
        String result = "";
        String inputLine;
        URL url = new URL("http://instance-data/latest/meta-data/" + metadata);
        URLConnection connection = url.openConnection();
        BufferedReader in = new BufferedReader(new InputStreamReader(
                connection.getInputStream()));
        try {
            while ((inputLine = in.readLine()) != null) {
                result += inputLine;
            }
        }
        finally {
            in.close();
        }
        return result;
    }

    public String retrieveName() throws IOException {
        final String instanceId = retrieveInstanceId();
        final Instance instance = describeInstanceById(instanceId);
        return getInstanceName(instance);
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
        request.setInstanceIds(asList(instanceId));
        return Iterables.getOnlyElement(describeInstances(request));
    }

    public Iterable<Instance> describeInstancesByName(final String name) {

        return describeInstances(
                new DescribeInstancesRequest().withFilters(
                        new Filter().withName("tag:name").withValues(name)));
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
