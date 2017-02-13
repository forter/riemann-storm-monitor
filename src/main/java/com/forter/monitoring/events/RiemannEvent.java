package com.forter.monitoring.events;

import backtype.storm.tuple.Tuple;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.storm.shade.org.apache.commons.lang.builder.ToStringBuilder;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class RiemannEvent {
    public String host;
    public String description;
    public String service;
    public String state;
    public long time;
    public double metric;
    public Float ttl;
    public List<String> tags;
    public Map<String, String> customAttributes;

    public transient Tuple tuple;

    /**
     * Initialize a new RiemannEvent
     */
    public RiemannEvent() {
        tags = Lists.newArrayList();
        customAttributes = Maps.newHashMap();
    }

    public RiemannEvent host(String host) {
        this.host = host;
        return this;
    }

    public RiemannEvent description(String description) {
        this.description = description;
        return this;
    }

    public RiemannEvent service(String service) {
        this.service = service;
        return this;
    }

    public RiemannEvent state(String state) {
        this.state = state;
        return this;
    }

    public RiemannEvent time(long time) {
        this.time = time;
        return this;
    }

    public RiemannEvent metric(double metric) {
        this.metric = metric;
        return this;
    }

    public RiemannEvent ttl(float ttl) {
        this.ttl = ttl;
        return this;
    }

    public RiemannEvent tags(String ... tags) {
        this.tags(Arrays.asList(tags));
        return this;
    }

    public RiemannEvent tags(Collection<String> tags) {
        if(tags != null) {
            this.tags.addAll(tags);
        }
        return this;
    }

    public RiemannEvent tuple(Tuple tuple) {
        if(tuple != null) {
            this.tuple = tuple;
        }
        return this;
    }

    public RiemannEvent attribute(String key, String value) {
        if(customAttributes == null) {
            customAttributes = Maps.newHashMap();
        }
        if (value != null) {
            customAttributes.put(key, value);
        }
        return this;
    }

    public RiemannEvent attribute(String key, Integer value) {
        return attribute(key, Integer.toString(value));
    }

    public RiemannEvent attribute(String key, Long value) {
        return attribute(key, Long.toString(value));
    }

    public RiemannEvent attribute(String key, Double value) {
        return attribute(key, Double.toString(value));
    }

    public RiemannEvent attributes(Map<String, String> attributes) {
        if(this.customAttributes == null) {
            this.customAttributes = attributes;
        } else {
            this.customAttributes.putAll(attributes);
        }
        return this;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}