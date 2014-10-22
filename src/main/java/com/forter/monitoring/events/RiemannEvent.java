package com.forter.monitoring.events;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.List;
import java.util.Map;

public class RiemannEvent {
    public String host;
    public String description;
    public String service;
    public String state;
    public long time;
    public double metric;
    public float ttl;
    public List<String> tags;
    public Map<String, String> customAttributes;


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
        this.tags = Lists.newArrayList(tags);
        return this;
    }

    public RiemannEvent tags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public RiemannEvent attribute(String key, String value) {
        if(customAttributes == null) {
            customAttributes = Maps.newHashMap();
        }
        customAttributes.put(key, value);
        return this;
    }

    public RiemannEvent attributes(Map<String, String> attributes) {
        this.customAttributes = attributes;
        return this;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}