package com.bazaarvoice.emopoller.busplus.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class BlockingBoundedExecutorTest {
    @Test public void testBounded() throws InterruptedException {
        final AtomicReference<LinkedBlockingQueue<Runnable>> queue = new AtomicReference<>(null);
        final ThreadPoolExecutor blockingBoundedExecutor = BlockingBoundedExecutorFactory.newExecutor(
            2, 2, 4, true,
            Duration.ofMinutes(1),
            "test-pool",
            (e, t) -> {
            },
            (p, q) -> queue.set(q)
        );

        final Semaphore spin = new Semaphore(0, true);
        final AtomicInteger current = new AtomicInteger(0);

        final Runnable wait = () -> {
            current.incrementAndGet();
            System.out.println("[" + Thread.currentThread().getName() + "] running");
            try {
                spin.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            current.decrementAndGet();
        };

        blockingBoundedExecutor.submit(wait); // should run immediately
        blockingBoundedExecutor.submit(wait); // should run immediately
        Thread.sleep(100L);
        Assert.assertEquals(current.get(), 2);
        blockingBoundedExecutor.submit(wait); // should wait in queue
        blockingBoundedExecutor.submit(wait); // should wait in queue
        blockingBoundedExecutor.submit(wait); // should wait in queue
        blockingBoundedExecutor.submit(wait); // should wait in queue
        Thread.sleep(100L);
        Assert.assertEquals(current.get(), 2);
        Assert.assertEquals(queue.get().size(), 4);

        final ExecutorService blockedExecutor = Executors.newSingleThreadExecutor();
        final AtomicBoolean lastSubmitted = new AtomicBoolean(false);
        final Future<?> submit = blockedExecutor.submit(() -> {
            blockingBoundedExecutor.submit(wait);
            lastSubmitted.set(true);
        });
        Thread.sleep(100);
        Assert.assertFalse(lastSubmitted.get());
        spin.release();
        Thread.sleep(100);
        Assert.assertTrue(lastSubmitted.get());
        Assert.assertEquals(current.get(), 2);
        Assert.assertEquals(queue.get().size(), 4);
        spin.release();
        spin.release();
        spin.release();
        Thread.sleep(100);
        Assert.assertEquals(current.get(), 2);
        Assert.assertEquals(queue.get().size(), 1);
        spin.release();
        Thread.sleep(100);
        Assert.assertEquals(current.get(), 2);
        Assert.assertEquals(queue.get().size(), 0);
        spin.release();
        Thread.sleep(100);
        Assert.assertEquals(current.get(), 1);
        Assert.assertEquals(queue.get().size(), 0);
        spin.release();
        Thread.sleep(100);
        Assert.assertEquals(current.get(), 0);
        Assert.assertEquals(queue.get().size(), 0);
    }
}
