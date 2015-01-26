package com.forter.monitoring;

import com.google.common.base.Optional;

import static com.google.common.base.Optional.of;

/**
* Created by reem on 1/26/15.
*/
class Latencies {
    private final Long executeStartNanos;

    private Long emitStartNanos = null;
    private Long emitEndNanos = null;

    public Latencies(Long executeStartNanos) {
        this.executeStartNanos = executeStartNanos;
    }

    public Long getExecuteLatencyNanos(Long endTimeNanos) {
        return endTimeNanos - executeStartNanos;
    }

    public Optional<Long> getEmitLatencyNanos() {
        if (emitStartNanos == null || emitEndNanos == null) {
            return Optional.absent();
        }
        return of(emitEndNanos - emitStartNanos);
    }

    public void setEmitStartNanos(Long emitStartNanos) {
        this.emitStartNanos = emitStartNanos;
    }

    public void setEmitEndNanos(Long emitEndNanos) {
        this.emitEndNanos = emitEndNanos;
    }
}
