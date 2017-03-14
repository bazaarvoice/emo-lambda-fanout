package com.bazaarvoice.emopoller.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MetricRegistrar {
    private static final Logger LOG = LoggerFactory.getLogger(MetricRegistrar.class);
    private static final ConcurrentHashMap<String, Boolean> registered = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Lock> locks = new ConcurrentHashMap<>();

    private final MetricRegistry metricRegistry;

    public MetricRegistrar(final MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    public void register(final String metricName, final Gauge<Integer> gauge) {
        register(metricName, ImmutableMap.of(), gauge);
    }

    public void register(final String metricName, final ImmutableMap<String, String> tags, final Gauge<Integer> gauge) {
        final String taggedName = getTaggedName(metricName, tags);
        registerGauge(taggedName, gauge);
    }

    public Timer timer(final String name, final ImmutableMap<String, String> tags) { return metricRegistry.timer(getTaggedName(name, tags)); }

    public Counter counter(final String name, final ImmutableMap<String, String> tags) { return metricRegistry.counter(getTaggedName(name, tags)); }



    public Histogram histogram(final String metricName, final ImmutableMap<String,String> tags) {
        return metricRegistry.histogram(getTaggedName(metricName, tags));
    }

    private String getTaggedName(final String metricName, final ImmutableMap<String, String> tags) {
        final String taggedName;
        if (tags.isEmpty()) {
            taggedName = metricName;
        } else {
            Preconditions.checkArgument(!(
                metricName.contains("[") ||
                    metricName.contains("]") ||
                    metricName.contains(":")
            ), "name contains illegal characters: <<" + metricName + ">>");
            final String tagString = Joiner.on(',').join(
                tags.entrySet()
                    .stream()
                    .map((entry) -> entry.getKey() + ":" + entry.getValue())
                    .iterator()
            );
            taggedName = metricName + "[" + tagString + "]";
        }
        return taggedName;
    }

    private void registerGauge(final String metricName, final Gauge<Integer> gauge) {
        locks.putIfAbsent(metricName, new ReentrantLock());
        final Lock lock = locks.get(metricName);
        lock.lock();
        try {
            final Boolean previousValue = registered.putIfAbsent(metricName, true);
            if (previousValue == null) {
                try {
                    metricRegistry.register(metricName, gauge);
                } catch (IllegalArgumentException e) {
                    LOG.warn("exception registering gauge: " + metricName, e);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void removeGauge(final String metricName, final ImmutableMap<String, String> tags) {
        removeGauge(getTaggedName(metricName, tags));
    }

    private void removeGauge(final String metricName) {
        locks.putIfAbsent(metricName, new ReentrantLock());
        final Lock lock = locks.get(metricName);
        lock.lock();
        try {
            final Boolean removed = registered.remove(metricName);
            if (removed != null) {
                metricRegistry.remove(metricName);
            }
        } finally {
            lock.unlock();
        }
    }
}
