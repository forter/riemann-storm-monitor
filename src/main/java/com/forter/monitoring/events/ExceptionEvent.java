package com.forter.monitoring.events;

import com.forter.monitoring.utils.MonitoringConstants;
import com.google.common.base.Throwables;

public class ExceptionEvent extends RiemannEvent {

    public ExceptionEvent(String exMessage) {
        this(exMessage, false);
    }

    public ExceptionEvent(String exMessage, boolean isRatioException) {
        super();
        this.description(exMessage);
        if (isRatioException) {
            this.tags.add("ratio-exception");
        } else {
            this.tags.add("exception");
        }
    }

    public ExceptionEvent(Throwable t) {
        this(t, false);
    }

    public ExceptionEvent(Throwable t, boolean isRatioException) {
        this(t.getMessage(), isRatioException);
        this.attribute(MonitoringConstants.ERROR_STACK_ATTR_ID, Throwables.getStackTraceAsString(t));
        this.attribute(MonitoringConstants.ERROR_TYPE_ATTR_ID, t.getClass().getSimpleName());
    }
}