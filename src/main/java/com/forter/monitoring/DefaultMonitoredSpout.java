package com.forter.monitoring;

import backtype.storm.topology.IRichSpout;
import com.forter.monitoring.eventSender.EventSender;
import com.forter.monitoring.utils.EventSenderSingleton;

public class DefaultMonitoredSpout extends MonitoredSpout {
    public DefaultMonitoredSpout(IRichSpout delegate) {
        super(delegate);
    }

    @Override
    protected EventSender getEventSender() {
        return EventSenderSingleton.getInstance().getSender();
    }
}
