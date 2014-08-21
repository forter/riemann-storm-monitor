package com.forter.monitoring;

import backtype.storm.topology.IComponent;

/**
 * Exposes class of inner bolt implementation
 */
public interface InnerBoltAware {
    Class<?> getDelegateClass();
}
