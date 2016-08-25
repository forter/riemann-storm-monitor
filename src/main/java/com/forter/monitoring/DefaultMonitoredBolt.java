package com.forter.monitoring;

import backtype.storm.topology.IRichBolt;
import com.forter.monitoring.eventSender.EventSender;
import com.forter.monitoring.utils.EventSenderSingleton;

/**
 * Created by reem on 21/12/2015.
 */
public class DefaultMonitoredBolt extends MonitoredBolt {
    public DefaultMonitoredBolt(IRichBolt delegate) {
        super(delegate);
    }

    public DefaultMonitoredBolt(IRichBolt delegate, CustomLatencyAttributesGenerator customAttributesGenerator) {
        super(delegate, 1, false);
        this.setCustomLatencyAttributesGenerator(customAttributesGenerator);
    }

    @Override
    protected EventSender getEventSender() {
        return EventSenderSingleton.getInstance().getSender();
    }
}
