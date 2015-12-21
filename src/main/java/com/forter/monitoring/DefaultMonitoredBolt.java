package com.forter.monitoring;

import backtype.storm.topology.IRichBolt;
import com.forter.monitoring.eventSender.EventSender;
import com.forter.monitoring.eventSender.LoggerEventSender;
import com.forter.monitoring.eventSender.RiemannEventSender;
import com.forter.monitoring.utils.EventSenderSingleton;
import com.forter.monitoring.utils.RiemannConnection;
import com.forter.monitoring.utils.RiemannDiscovery;

/**
 * Created by reem on 21/12/2015.
 */
public class DefaultMonitoredBolt extends MonitoredBolt {
    public DefaultMonitoredBolt(IRichBolt delegate) {
        super(delegate);
    }

    public DefaultMonitoredBolt(IRichBolt delegate, CustomLatencyAttributesGenerator customAttributesGenerator) {
        super(delegate, customAttributesGenerator);
    }

    @Override
    protected EventSender getEventSender() {
        return EventSenderSingleton.getInstance().getSender();
    }
}
