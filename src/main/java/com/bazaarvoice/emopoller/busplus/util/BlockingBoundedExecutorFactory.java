package com.bazaarvoice.emopoller.busplus.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BlockingBoundedExecutorFactory {
    private BlockingBoundedExecutorFactory() {}

    public interface Hook {
        void onCreate(final ThreadPoolExecutor threadPoolExecutor, final LinkedBlockingQueue<Runnable> bufferedWork);
    }

    public static ThreadPoolExecutor newExecutor(final int core, final int max,
                                                 final int bufferSize, final boolean fair,
                                                 final Duration keepAlive,
                                                 final String name,
                                                 final Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
                                                 final Hook onCreate) {
        final LinkedBlockingQueue<Runnable> bufferedWork = new LinkedBlockingQueue<Runnable>(bufferSize) {
            // alter the semantics so it actually blocks
            @SuppressWarnings("NullableProblems") @Override public boolean offer(final Runnable runnable) {
                try {
                    super.put(runnable);
                    return true;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
            core, max, //core, max pool size
            keepAlive.toMillis(), TimeUnit.MILLISECONDS,  // keep alive time
            bufferedWork, // work queue
            new ThreadFactoryBuilder() // thread factory
                .setNameFormat(name + "-%d")
                .setUncaughtExceptionHandler(uncaughtExceptionHandler)
                .build());
        onCreate.onCreate(threadPoolExecutor, bufferedWork);

        return threadPoolExecutor;
    }
}
