package com.forter.monitoring;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

public class EventProperties {
    private Map<String, String> attributes;
    private Set<String> tags;

    public EventProperties() {
        attributes = Maps.newHashMap();
        tags = Sets.newHashSet();
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public Set<String> getTags() {
        return tags;
    }
}
