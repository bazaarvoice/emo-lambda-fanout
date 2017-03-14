package com.bazaarvoice.emopoller.emo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.fail;

public class DataStoreUtilsTest {
    private static final Logger LOG = LoggerFactory.getLogger(DataStoreUtilsTest.class);

    @Test public void doneIsDone() {
        final AtomicBoolean done = new AtomicBoolean(false);
        final DataStoreUtils.BlockingTraversableOnce<String> traversableOnce = new DataStoreUtils.BlockingTraversableOnce<>(Duration.ZERO, done::get);

        final ImmutableList<String> of = ImmutableList.of("a", "b", "c");

        traversableOnce.addAll(of);

        done.set(true);

        try {
            traversableOnce.addAll(ImmutableList.of("x"));
            fail();
        } catch (IllegalStateException e) {
            Assert.assertEquals(e.getMessage(), "Cannot add more items after done returns true.");
        }
    }

    @Test public void singleThreadTraverse() {
        final AtomicBoolean done = new AtomicBoolean(false);
        final DataStoreUtils.BlockingTraversableOnce<String> traversableOnce = new DataStoreUtils.BlockingTraversableOnce<>(Duration.ZERO, done::get);

        final ImmutableList<String> input = ImmutableList.of("a", "b", "c");
        final ImmutableList<String> input2 = ImmutableList.of("d", "e");

        traversableOnce.addAll(input);
        traversableOnce.addAll(input2);

        done.set(true);

        Assert.assertEquals(ImmutableSet.copyOf(traversableOnce), Sets.union(ImmutableSet.copyOf(input), ImmutableSet.copyOf(input2)));
    }

    @Test public void interleavedTraverse() {
        final AtomicBoolean done = new AtomicBoolean(false);
        final DataStoreUtils.BlockingTraversableOnce<String> traversableOnce = new DataStoreUtils.BlockingTraversableOnce<>(Duration.ZERO, done::get);
        final Iterator<String> iterator = traversableOnce.iterator();

        final ImmutableList<String> input = ImmutableList.of("a", "b", "c");
        traversableOnce.addAll(input);

        Assert.assertEquals(iterator.next(), "a");
        Assert.assertEquals(iterator.next(), "b");
        Assert.assertEquals(iterator.next(), "c");
        final ImmutableList<String> input2 = ImmutableList.of("d", "e");
        traversableOnce.addAll(input2);
        Assert.assertEquals(iterator.next(), "d");
        Assert.assertEquals(iterator.next(), "e");
        done.set(true);

        Assert.assertEquals(iterator.hasNext(), false);

        try {
            iterator.next();
            fail();
        } catch (NoSuchElementException e) {
            // pass
        }
    }

    @Test public void blockingTraverse() throws InterruptedException {
        final AtomicBoolean done = new AtomicBoolean(false);
        final DataStoreUtils.BlockingTraversableOnce<String> traversableOnce = new DataStoreUtils.BlockingTraversableOnce<>(Duration.ofMillis(100), done::get);
        final Iterator<String> iterator = traversableOnce.iterator();

        final ExecutorService pollExec = Executors.newSingleThreadExecutor();

        final AtomicBoolean a = new AtomicBoolean(false);
        final AtomicBoolean b = new AtomicBoolean(false);
        final AtomicBoolean c = new AtomicBoolean(false);
        final AtomicBoolean d = new AtomicBoolean(false);
        final AtomicBoolean e = new AtomicBoolean(false);
        final AtomicBoolean nse = new AtomicBoolean(false);
        pollExec.submit(() -> {
            try {
                System.out.println("START");
                Assert.assertEquals(iterator.next(), "a");
                System.out.println("A");
                a.set(true);
                Assert.assertEquals(iterator.next(), "b");
                System.out.println("B");
                b.set(true);
                Assert.assertEquals(iterator.next(), "c");
                System.out.println("C");
                c.set(true);
                Assert.assertEquals(iterator.next(), "d");
                System.out.println("D");
                d.set(true);
                Assert.assertEquals(iterator.next(), "e");
                System.out.println("E");
                e.set(true);
                try {
                    iterator.next();
                    fail();
                } catch (NoSuchElementException ignore) {
                    System.out.println("NSE");
                    nse.set(true);
                }
            } catch (Exception err) {
                LOG.error("Unhandled exception in test", err);
            }
        });

        Assert.assertFalse(a.get());
        final ImmutableList<String> input = ImmutableList.of("a", "b", "c");
        traversableOnce.addAll(input);
        Thread.sleep(20);

        Assert.assertTrue(a.get());
        Assert.assertTrue(b.get());
        Assert.assertTrue(c.get());
        Assert.assertFalse(d.get());

        final ImmutableList<String> input2 = ImmutableList.of("d", "e");
        traversableOnce.addAll(input2);
        Thread.sleep(20);
        Assert.assertTrue(d.get());
        Assert.assertTrue(e.get());
        Assert.assertFalse(nse.get());

        done.set(true);
        Thread.sleep(20);
        Assert.assertTrue(nse.get());
    }
}
