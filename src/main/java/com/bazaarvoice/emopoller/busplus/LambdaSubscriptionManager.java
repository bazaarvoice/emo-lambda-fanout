package com.bazaarvoice.emopoller.busplus;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emopoller.EmoPollerConfiguration;
import com.bazaarvoice.emopoller.busplus.kms.ApiKeyCrypto;
import com.bazaarvoice.emopoller.busplus.lambda.InsufficientPermissionsException;
import com.bazaarvoice.emopoller.busplus.lambda.LambdaInvocation;
import com.bazaarvoice.emopoller.busplus.lambda.LambdaSubscriptionDAO;
import com.bazaarvoice.emopoller.busplus.lambda.NoSuchFunctionException;
import com.bazaarvoice.emopoller.busplus.lambda.model.ConstructedLambdaSubscription;
import com.bazaarvoice.emopoller.busplus.lambda.model.LambdaSubscription;
import com.bazaarvoice.emopoller.util.HashUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class LambdaSubscriptionManager implements Managed {
    private static final Logger LOG = LoggerFactory.getLogger(LambdaSubscriptionManager.class);
    private final LambdaSubscriptionDAO lambdaSubscriptionDAO;
    private final LambdaInvocation lambdaInvocation;
    private final ApiKeyCrypto apiKeyCrypto;

    private final String subscriptionPrefix;
    private final ProcessPool processPool;

    private final ConcurrentHashMap<String, SubscriptionPollerFactory.SubscriptionPoller> pollers;

    private volatile boolean started = false;
    private ReentrantLock startedLock = new ReentrantLock();

    @Inject
    public LambdaSubscriptionManager(final EmoPollerConfiguration.EmoConfiguration emoConfiguration,
                                     final LambdaInvocation lambdaInvocation,
                                     final LambdaSubscriptionDAO lambdaSubscriptionDAO,
                                     final ApiKeyCrypto apiKeyCrypto,
                                     final SubscriptionPollerFactory subscriptionPollerFactory,
                                     final ProcessPool processPool) {
        this.lambdaSubscriptionDAO = lambdaSubscriptionDAO;
        this.subscriptionPrefix = emoConfiguration.getSubscriptionPrefix();
        this.processPool = processPool;
        this.lambdaInvocation = lambdaInvocation;
        this.apiKeyCrypto = apiKeyCrypto;

        this.pollers = new ConcurrentHashMap<>();

        lambdaSubscriptionDAO.registerWatcher("poller-watcher", lambdaSubscription -> {
            final SubscriptionPollerFactory.SubscriptionPoller poller;
            try {
                poller = subscriptionPollerFactory.produce(lambdaSubscription.getEnvironment(), lambdaSubscription.getLambdaArn());
            } catch (IllegalArgumentException e) {
                LOG.warn("Exception creating poller for subscription: ", e);
                return;
            }
            pollers.putIfAbsent(lambdaSubscription.getId(), poller);
            startedLock.lock();
            try {
                if (started) {
                    pollers.get(lambdaSubscription.getId()).startAsync();
                }
            } catch (IllegalStateException e) {
                //noinspection StatementWithEmptyBody
                if (e.getMessage().endsWith("has already been started")) {
                    // ignore
                } else {
                    throw e;
                }
            } finally {
                startedLock.unlock();
            }
        });
    }

    public void register(final String environment, final String lambdaArn, final Condition condition, final Duration claimTtl, final Integer batchSize, final String delegateApiKey)
        throws NoSuchFunctionException, InsufficientPermissionsException, IllegalArgumentException {

        Preconditions.checkArgument(environment.matches("[a-z]+"), "environment must be lowercase alpha only.");
        lambdaInvocation.check(lambdaArn);

        final LambdaSubscription subscription = lambdaSubscriptionDAO.get(environment, lambdaArn);

        final String subscriptionName;
        if (subscription == null) {
            subscriptionName = subscriptionPrefix + "-" + UUID.randomUUID();
        } else {
            subscriptionName = subscription.getSubscriptionName();
        }

        final String cypherTextDelegateApiKey = apiKeyCrypto.encrypt(delegateApiKey, subscriptionName, lambdaArn);

        final String id = lambdaSubscriptionDAO.saveAndNotifyWatchers(
            new ConstructedLambdaSubscription(environment, subscriptionName, lambdaArn, condition.toString(), claimTtl, batchSize, delegateApiKey, cypherTextDelegateApiKey, true)
        );

        pollers.get(id).ensureSubscribed();

        // (re)set error count to zero on (re)registration
        processPool.resetErrorCount(id);
    }

    public void deactivate(final String environment, final String lambdaArn, final String delegateApiKey) {
        final LambdaSubscription subscription = get(environment, lambdaArn, delegateApiKey); // checks permission
        lambdaSubscriptionDAO.deactivate(subscription.getId());
    }

    public void activate(final String environment, final String lambdaArn, final String delegateApiKey) {
        final LambdaSubscription subscription = get(environment, lambdaArn, delegateApiKey); // checks permission
        lambdaSubscriptionDAO.activate(subscription.getId());
    }

    public static class UnauthorizedException extends RuntimeException {}

    public List<LambdaSubscription> getAll(final String environment, final String key) {
        final ImmutableList.Builder<LambdaSubscription> result = ImmutableList.builder();
        for (LambdaSubscription subscription : lambdaSubscriptionDAO.getAll(environment)) {
            if (subscription != null) {
                if (HashUtil.verifyArgon2(subscription.getDelegateApiKeyHash(), key)) {
                    result.add(subscription);
                }
            }
        }
        return result.build();
    }

    public Integer size(final String environment, final String lambdaArn, final String delegateApiKey) {
        final LambdaSubscription subscription = get(environment, lambdaArn, delegateApiKey);
        return pollers.get(subscription.getId()).size();
    }

    public LambdaSubscription get(final String environment, final String lambdaArn, final String delegateApiKey) {
        final LambdaSubscription subscription = lambdaSubscriptionDAO.get(environment, lambdaArn);
        if (HashUtil.verifyArgon2(subscription.getDelegateApiKeyHash(), delegateApiKey)) {
            return subscription;
        } else {
            throw new UnauthorizedException();
        }
    }

    @Override public void start() throws Exception {
        startedLock.lock();
        try {
            pollers.values().forEach(SubscriptionPollerFactory.SubscriptionPoller::start);
            started = true;
        } finally {
            startedLock.unlock();
        }
    }

    @Override public void stop() throws Exception {
        pollers.values().forEach(SubscriptionPollerFactory.SubscriptionPoller::stop);
    }
}
