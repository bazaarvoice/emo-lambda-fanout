package com.bazaarvoice.emopoller.busplus.kms;

import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.model.DecryptRequest;
import com.amazonaws.services.kms.model.DecryptResult;
import com.amazonaws.services.kms.model.EncryptRequest;
import com.amazonaws.services.kms.model.EncryptResult;
import com.bazaarvoice.emopoller.metrics.MetricRegistrar;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.concurrent.ExecutionException;

public class ApiKeyCrypto {
    private final String cmk;
    private final AWSKMS kms;
    private final LoadingCache<CacheKey, String> cache;
    private final MetricRegistrar metricRegistrar;

    private static final Logger LOG = LoggerFactory.getLogger(ApiKeyCrypto.class);

    @Inject public ApiKeyCrypto(@Named("cmk") final String cmk,
                                final AWSKMS kms,
                                final MetricRegistrar metricRegistrar) {
        this.cmk = cmk;
        this.kms = kms;
        this.cache = CacheBuilder.newBuilder().maximumSize(10_000).build(new CacheLoader<CacheKey, String>() {
            @Override public String load(@SuppressWarnings("NullableProblems") final CacheKey key) throws Exception {
                Preconditions.checkNotNull(key);
                LOG.info("decrypting key {}", key);
                final ByteBuffer byteBuffer = ByteBuffer.wrap(Base64.getDecoder().decode(key.apiKeyCypherText));
                final DecryptResult decrypt = kms.decrypt(new DecryptRequest()
                    .withCiphertextBlob(byteBuffer)
                    .withEncryptionContext(ImmutableMap.of(
                        "subscriptionName", key.subscriptionName,
                        "lambdaArn", key.lambdaArn
                    ))
                );

                return new String(decrypt.getPlaintext().array());
            }
        });
        this.metricRegistrar = metricRegistrar;
    }


    public String encrypt(final String apiKey, final String subscriptionName, final String lambdaArn) {
        final EncryptResult encryptResult = kms.encrypt(new EncryptRequest()
            .withKeyId(cmk)
            .withEncryptionContext(ImmutableMap.of(
                "subscriptionName", subscriptionName,
                "lambdaArn", lambdaArn
            ))
            .withPlaintext(ByteBuffer.wrap(apiKey.getBytes()))
        );

        return Base64.getEncoder().encodeToString(encryptResult.getCiphertextBlob().array());
    }

    public String decryptCustom(final String cypherTextKey, final ImmutableMap<String, String> encryptionContext) {
        LOG.info("decrypting key {}", cypherTextKey);
        final ByteBuffer byteBuffer = ByteBuffer.wrap(Base64.getDecoder().decode(cypherTextKey));
        final DecryptResult decrypt = kms.decrypt(new DecryptRequest()
            .withCiphertextBlob(byteBuffer)
            .withEncryptionContext(encryptionContext)
        );

        return new String(decrypt.getPlaintext().array());
    }

    public String decrypt(final String cypherTextKey, final String subscriptionName, final String lambdaArn) {
        final Timer.Context time = metricRegistrar.timer("emo_lambda_fanout.crypto.decrypt.time", ImmutableMap.of()).time();
        try {
            return cache.get(new CacheKey(cypherTextKey, subscriptionName, lambdaArn));
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            time.stop();
        }
    }

    private static class CacheKey {
        private final String apiKeyCypherText;
        private final String subscriptionName;
        private final String lambdaArn;

        private CacheKey(final String apiKeyCypherText, final String subscriptionName, final String lambdaArn) {
            this.apiKeyCypherText = apiKeyCypherText;
            this.subscriptionName = subscriptionName;
            this.lambdaArn = lambdaArn;
        }

        @Override public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final CacheKey cacheKey = (CacheKey) o;

            return apiKeyCypherText.equals(cacheKey.apiKeyCypherText) && subscriptionName.equals(cacheKey.subscriptionName) && lambdaArn.equals(cacheKey.lambdaArn);

        }

        @Override public int hashCode() {
            int result = apiKeyCypherText.hashCode();
            result = 31 * result + subscriptionName.hashCode();
            result = 31 * result + lambdaArn.hashCode();
            return result;
        }

        @Override public String toString() {
            return "CacheKey{" +
                "apiKeyCypherText='" + apiKeyCypherText.substring(0, 10) + "...'" +
                ", subscriptionName='" + subscriptionName + '\'' +
                ", lambdaArn='" + lambdaArn + '\'' +
                '}';
        }
    }
}
