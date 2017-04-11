package com.bazaarvoice.emopoller.busplus.lambda;

import com.bazaarvoice.emopoller.busplus.lambda.model.LambdaSubscription;
import io.dropwizard.lifecycle.Managed;

import java.util.Set;

public interface LambdaSubscriptionDAO extends Managed {
    interface Watcher {
        void onUpdate(final LambdaSubscription lambdaSubscription);
    }

    @SuppressWarnings("SameParameterValue")
    void registerWatcher(final String name, final Watcher watcher);

    String saveAndNotifyWatchers(final LambdaSubscription lambdaSubscription);

    Set<LambdaSubscription> getAll(final String tenant);

    LambdaSubscription get(final String tenant, final String lambdaArn);

    void deactivate(final String id);

    void activate(final String id);
}
