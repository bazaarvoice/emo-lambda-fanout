package com.bazaarvoice.emopoller;

import com.bazaarvoice.emopoller.busplus.LambdaSubscriptionManager;
import com.bazaarvoice.emopoller.busplus.lambda.LambdaSubscriptionDAO;
import com.bazaarvoice.emopoller.busplus.lambda.LambdaSubscriptionDAOImpl;
import com.bazaarvoice.emopoller.busplus.lambda.LambdaThroughputTest;
import com.bazaarvoice.emopoller.metrics.MetricsTelemetry;
import com.bazaarvoice.emopoller.resource.AuthorizationRequestFilter;
import com.bazaarvoice.emopoller.resource.PollerResource;
import com.bazaarvoice.emopoller.tools.HashKeyCommand;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class EmoPollerApp extends Application<EmoPollerConfiguration> {
//    private static final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    public static void main(String[] args) throws Exception {
        //TODO record thread pause times
//        threadMXBean.setThreadCpuTimeEnabled(true);
//        threadMXBean.setThreadContentionMonitoringEnabled(true);
        new EmoPollerApp().run(args);
    }

    @Override public String getName() {
        return "emo-lambda-fanout";
    }

    @Override public void initialize(final Bootstrap<EmoPollerConfiguration> bootstrap) {
        // Enable variable substitution with environment variables
        bootstrap.setConfigurationSourceProvider(
            new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(),
                new EnvironmentVariableSubstitutor(false)
            )
        );

        bootstrap.addCommand(new HashKeyCommand());
        bootstrap.addCommand(new LambdaThroughputTest());
    }

    @Override public void run(final EmoPollerConfiguration configuration, final Environment environment) throws Exception {
        final Injector injector = Guice.createInjector(
            Stage.PRODUCTION /*eagerly inject and load everything*/,
            new EmoPollerModule(getName(), configuration, environment.metrics(), environment.healthChecks()));

        environment.jersey().register(injector.getInstance(AuthorizationRequestFilter.class));
        environment.jersey().register(injector.getInstance(PollerResource.class));

        environment.lifecycle().manage(injector.getInstance(LambdaSubscriptionDAO.class));

        environment.lifecycle().manage(injector.getInstance(LambdaSubscriptionManager.class));

        environment.lifecycle().manage(injector.getInstance(MetricsTelemetry.class));
    }
}
