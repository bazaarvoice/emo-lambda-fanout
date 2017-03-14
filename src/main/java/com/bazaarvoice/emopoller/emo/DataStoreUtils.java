package com.bazaarvoice.emopoller.emo;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class DataStoreUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DataStoreUtils.class);

    private static final ListeningExecutorService pscanPool = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(
            100,
            new ThreadFactoryBuilder()
                .setNameFormat("pscan-%d")
                .setUncaughtExceptionHandler((thread, throwable) -> LOG.error("[" + thread.getName() + "] Uncaught exception in pscan.", throwable))
                .build()
        )
    );

    static class BlockingTraversableOnce<T> implements Iterable<T> {
        private AtomicBoolean traversed = new AtomicBoolean(false);
        private final Supplier<Boolean> done;
        private final Duration getNextElementTimeout;
        private final Queue<T> buffer = new ConcurrentLinkedQueue<>();

        private final Iterator<T> iterator = new AbstractIterator<T>() {
            // can throw TimeoutException (wrapped in RuntimeException)
            @Override protected T computeNext() {
                final long start = System.currentTimeMillis();

                // spin until the buffer is nonempty or we are done and drained.
                while (buffer.isEmpty()) {
                    if (done.get() && buffer.isEmpty() /*yep, check the buffer again AFTER done==true*/) {
                        endOfData();
                        return null;
                    } else {
                        final long duration = System.currentTimeMillis() - start;
                        if (duration > getNextElementTimeout.toMillis()) {
                            throw new RuntimeException(new TimeoutException("duration: " + duration));
                        }
                        sleepQuietly(10L);
                    }
                }

                return buffer.poll();
            }
        };

        BlockingTraversableOnce(final Duration getNextElementTimeout, final Supplier<Boolean> doneFn) {
            this.done = doneFn;
            this.getNextElementTimeout = getNextElementTimeout;
        }

        void addAll(Collection<T> items) {
            Preconditions.checkState(!done.get(), "Cannot add more items after done returns true.");
            buffer.addAll(items);
        }

        @Override public Iterator<T> iterator() {
            Preconditions.checkState(!traversed.getAndSet(true), "Cannot return the iterator more than once.");
            return iterator;
        }
    }

    public static Iterable<JsonNode> pscan(DataStoreClient client, String table, String apiKey) {

        final Collection<String> splits = client.getSplits(table, 100, apiKey);

        final ConcurrentLinkedQueue<ListenableFuture<?>> futures = new ConcurrentLinkedQueue<>();
        final BlockingTraversableOnce<JsonNode> result = new BlockingTraversableOnce<>(
            Duration.ofMinutes(1),
            () -> {
                final ListenableFuture<?> allFuture = Futures.allAsList(futures);
                final boolean done = allFuture.isDone();
                if (done) {
                    // surface any exceptions.
                    getOrThrow(allFuture, Duration.ofMinutes(1));
                }
                return done;
            }
        );

        for (String split : splits) {
            final ListenableFuture<?> listenableFuture = pscanPool.submit(
                () -> {
                    String fromKey = null;
                    ImmutableList<JsonNode> nodes;
                    do {
                        nodes = ImmutableList.copyOf(client.getSplit(table, split, fromKey, 100, apiKey));
                        if (!nodes.isEmpty()) {
                            fromKey = nodes.get(nodes.size() - 1).get("~id").textValue();
                            result.addAll(nodes);
                        }
                    } while (!nodes.isEmpty());
                }
            );

            futures.add(listenableFuture);
        }

        return result;
    }

    private static <T> T getOrThrow(final ListenableFuture<T> Future, final Duration timeout) {
        try {
            return Future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw Throwables.propagate(e);
        }
    }

    private static void sleepQuietly(long sleepMillis) {
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }
}
