package com.forter.monitoring;

import org.apache.storm.tuple.Tuple;
import com.forter.monitoring.eventSender.EventSender;
import com.forter.monitoring.eventSender.RiemannEventSender;
import com.forter.monitoring.events.RiemannEvent;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.cache.*;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/*
This singleton class centralizes the storm-monitoring functions.
The monitored bolts and spouts will use the functions in this class.
 */
public class Monitor implements EventSender {
    static private final Logger logger = LoggerFactory.getLogger(Monitor.class);

    public static final String BOLT_EXCLUSIONS_EXTRA_ACK_ERROR_PROP = "monitoring.report.exclusions.extra-ack";
    public static final String IGNORED_STREAMS_PROP = "monitoring.stream.ignore";

    private static final int MAX_CONCURRENCY_DEFAULT = 2;

    private static final Long MAX_SIZE_DEFAULT = getEnv("LATENCY_REPORTING_MAP_MAX_SIZE", 1000L);
    private static final long MAX_TIME_DEFAULT = getEnv("LATENCY_REPORTING_MAX_WAIT", 60L);

    private static final long PERIODIC_CLEANUP_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(5);

    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final Random randomGenerator = new Random();

    private final EventSender eventSender;
    private final Cache<Object, Latencies> latenciesPerId;
    private final Map<String, String> customAttributes;
    private final Object cacheLock;
    private final Set<String> extraAckReportingExclusions;
    private final String boltService;
    private final Set<String> ignoredStreams;
    private final LatencyMonitorEventCreator latencyMonitorEventCreator;

    private int maxConcurrency;
    private long maxSize;
    private long maxTime;

    public Monitor(Map conf, final String boltService, EventSender eventSender, LatencyMonitorEventCreator latencyMonitorEventCreator) {
        this.latenciesPerId = createCache(conf, boltService);

        this.customAttributes = extractCustomEventAttributes(conf);
        this.eventSender = eventSender;
        this.cacheLock = new Object();
        this.boltService = boltService;

        // Generate an initial delay randomizer so that not all bolt cleanups would run in the same time. Randomizer
        // value can be between negative and positive PERIODIC_CLEANUP_INTERVAL_MILLIS/2
        long randomMillis = (randomGenerator.nextLong() % (PERIODIC_CLEANUP_INTERVAL_MILLIS/2));

        this.extraAckReportingExclusions = getListConfigurationPropery(conf, BOLT_EXCLUSIONS_EXTRA_ACK_ERROR_PROP);
        this.ignoredStreams = getListConfigurationPropery(conf, IGNORED_STREAMS_PROP);

        if (latencyMonitorEventCreator != null) {
            this.latencyMonitorEventCreator = latencyMonitorEventCreator;
        } else {
            this.latencyMonitorEventCreator = new DefaultLatencyMonitorEventCreator();
        }

        scheduler.scheduleAtFixedRate(
                new Runnable() {
                    @Override
                    public void run() {
                        synchronized (cacheLock) {
                            latenciesPerId.cleanUp();
                        }
                    }
                },
                PERIODIC_CLEANUP_INTERVAL_MILLIS + randomMillis,
                PERIODIC_CLEANUP_INTERVAL_MILLIS,
                TimeUnit.MILLISECONDS);
    }

    private Set<String> getListConfigurationPropery(Map conf, String configProp) {
        final String prop = (String) conf.get(configProp);

        Set<String> result = Sets.newHashSet();

        if (!Strings.isNullOrEmpty(prop)) {
            Iterables.addAll(result, Splitter.on(",").split(prop));
        }

        return result;
    }

    public Monitor() {
        this(new HashMap(), "", null, null);
    }

    private Cache<Object, Latencies>  createCache(Map conf, final String boltService) {
        initCacheConfig(conf);

        return CacheBuilder.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(maxTime, TimeUnit.SECONDS)
                .concurrencyLevel(maxConcurrency)
                .removalListener(new RemovalListener<Object, Latencies>() {
                    // The on removal callback is not instantly called on removal, but I  hope it will be called
                    // eventually. see:
                    // http://stackoverflow.com/questions/21986551/guava-cachebuilder-doesnt-call-removal-listener
                    @Override
                    public void onRemoval(RemovalNotification<Object, Latencies> notification) {
                        if (notification.getCause() != RemovalCause.EXPLICIT) {
                            send(latencyMonitorEventCreator.createExpiryRemovalEvents(notification, boltService));
                        }
                    }
                })
                .build();
    }

    private void initCacheConfig(Map conf) {
        Object maxSizeConf = conf.get("topology.monitoring.latencies.map.maxSize");
        Object maxTimeConf = conf.get("topology.monitoring.latencies.map.maxTimeSeconds");
        Object maxConcurrencyConf = conf.get("topology.monitoring.latencies.map.maxConcurrency");

        maxSize = (maxSizeConf == null ? MAX_SIZE_DEFAULT : (long) maxSizeConf);
        maxTime = (maxTimeConf == null ? MAX_TIME_DEFAULT : (long) maxTimeConf);
        maxConcurrency = (maxConcurrencyConf == null ? MAX_CONCURRENCY_DEFAULT: Ints.checkedCast((long) maxConcurrencyConf));

        logger.info("Initializing latencies map with parameters maxSize: {}, maxTimeSeconds: {}, maxConcurrency: {}",
                maxSize, maxTime, maxConcurrency);
    }

    public void startExecute(Object latencyId, Tuple tuple, String service) {
        registerLatency(latencyId, LatencyType.EXECUTE, true, service, tuple, null, null);
    }

    public void endExecute(Object latencyId, EventProperties attributes, boolean success) {
        registerLatency(latencyId, LatencyType.EXECUTE, false, null, null, attributes, success);
    }

    public void ignoreExecute(Object latencyId) {
        latenciesPerId.invalidate(latencyId);
    }

    public void startLatency(Object latencyId, LatencyType type) {
        registerLatency(latencyId, type, true, null, null, null, null);
    }

    public void endLatency(Object latencyId, LatencyType type) {
        registerLatency(latencyId, type, false, null, null, null, null);

    }

    public void send(RiemannEvent event) {
        event.attributes(customAttributes);

        if (event.tuple != null) {
            HashMap<String, String> attributes = Maps.newHashMap();

            if (event.tuple.contains("_queueTime") && event.customAttributes.containsKey("startTimeMillis")) {
                final String queueTimeString = event.tuple.getValueByField("_queueTime").toString();
                if (!queueTimeString.equals("unknown")) {
                    try {
                        final long queueTime = Long.valueOf(queueTimeString);
                        final Long startTimeMillis = Long.valueOf(event.customAttributes.get("startTimeMillis"));
                        long elapsed = startTimeMillis - queueTime;
                        attributes.put("timeElapsedToStart", Long.toString(elapsed));
                        attributes.put("absoluteLatency", Double.toString(elapsed + event.metric));
                    } catch (NumberFormatException nfe) { /* ignore */ }
                }
            }

            event.customAttributes.put("tupleReceivedComponent", event.tuple.getSourceComponent());
            event.customAttributes.put("tupleReceivedStream", event.tuple.getSourceStreamId());

            event.attributes(attributes);
        }

        eventSender.send(event);
    }

    private void registerLatency(Object latencyId, LatencyType type, boolean isStart, String service, Tuple tuple,
                                 EventProperties properties, Boolean success) {
        final long nanos = System.nanoTime();
        Latencies latencies;
        synchronized (cacheLock) {
            switch(type) {
                case EXECUTE:
                    if (isStart) {
                        latencies = new Latencies(nanos, service, tuple);

                        latenciesPerId.put(latencyId, latencies);

                        if (logger.isDebugEnabled()) {
                            logger.debug("Monitoring latency for key {}", latencyId);
                        }
                    } else {
                        latencies = latenciesPerId.getIfPresent(latencyId);
                        if (latencies != null &&  latencies.setEndNanos(type, nanos) && latencies.getLatencyNanos(type).isPresent()) {
                            latenciesPerId.invalidate(latencyId);

                            long endTimeMillis = System.currentTimeMillis();
                            long elapsedMillis = NANOSECONDS.toMillis(latencies.getLatencyNanos(type).get());

                            Iterable<RiemannEvent> event = this.latencyMonitorEventCreator.createLatencyEvents(success, latencies, endTimeMillis, elapsedMillis, properties);

                            send(event);

                            final Optional<Long> emitLatencyNanos = latencies.getLatencyNanos(LatencyType.EMIT);
                            if (emitLatencyNanos.isPresent()) {
                                final long emitMillis = NANOSECONDS.toMillis(emitLatencyNanos.get());

                                if (emitMillis >= 5) {
                                    send(latencyMonitorEventCreator.createEmitLatencyEvents(emitMillis, this.boltService, tuple));
                                }
                            }

                            if (logger.isDebugEnabled()) {
                                logger.debug("Monitored latency {} for key {}", elapsedMillis, latencyId);
                            }
                        } else {
                            if (!extraAckReportingExclusions.contains(this.boltService)) {
                                send(latencyMonitorEventCreator.createMonitorKeyMissingEvents(service, latencyId));
                            } else {
                                logger.trace("Excluded event for non recognized key in latency monitor {}.", latencyId);
                            }
                        }
                    }
                    break;
                default:
                    latencies = latenciesPerId.getIfPresent(latencyId);
                    if (latencies != null) {
                        if (isStart) {
                            latencies.setStartNanos(type, nanos);
                        } else {
                            latencies.setEndNanos(type, nanos);
                        }
                    }
                    break;
            }
        }
    }

    private void send(Iterable<RiemannEvent> events) {
        for (RiemannEvent event : events) {
            send(event);
        }
    }

    private Map<String,String> extractCustomEventAttributes(Map conf) {
        if (conf.containsKey("topology.riemann.attributes")) {
            Object attributes = conf.get("topology.riemann.attributes");
            if (attributes instanceof String) {
                String attributesString = (String) attributes;
                return parseAttributesString(attributesString);
            } else {
                logger.warn("Wrong type of custom attributes for riemann, supposed to be String but is {}", attributes.getClass());
            }
        }

        return new HashMap<>();
    }

    /**
     * @return riemann client if discovered on aws.
     */
    public Optional<RiemannEventSender> getRiemannEventSender() {
        if (eventSender instanceof RiemannEventSender) {
            return Optional.of((RiemannEventSender)eventSender);
        }
        return Optional.absent();
    }

    public static Map<String,String> parseAttributesString(String attributesString) {
        Map<String, String> attributesMap = new HashMap<>();

        for (String attribute : attributesString.split(",")) {
            String[] keyValue = attribute.split("=");
            if (keyValue.length != 2) {
                logger.warn("Bad format of custom attribute - {}", keyValue);
                continue;
            }
            attributesMap.put(keyValue[0], keyValue[1]);
        }

        return attributesMap;
    }

    public boolean shouldMonitor(Tuple input) {
        return !this.ignoredStreams.contains(input.getSourceStreamId());
    }

    private static Long getEnv(String name, Long defaultValue) {
        String value = System.getenv(name);

        if (value == null)
            return defaultValue;

        return Long.parseLong(value);
    }
}
