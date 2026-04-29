package com.deepgram.sagemaker;

import java.time.Duration;

import software.amazon.awssdk.regions.Region;

/**
 * Configuration for connecting to a Deepgram model hosted on SageMaker.
 *
 * <p>Defaults are tuned for high-burst workloads (large numbers of streams
 * opened in a tight loop against an endpoint that may need to scale up). They
 * are intentionally more lenient than the AWS SDK Netty defaults so that
 * 200&ndash;500-stream bursts don't trip connect-acquire / connect-handshake
 * timeouts before the endpoint has had a chance to accept the inbound TLS
 * handshakes. Tighten them if you want fail-fast behavior in low-latency
 * pipelines.
 */
public class SageMakerConfig {

    /** AWS Netty default is 2&nbsp;s. Set to 30&nbsp;s so cold endpoints under burst load can accept TLS. */
    public static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(30);

    /** AWS Netty default is 10&nbsp;s. Set to 60&nbsp;s so a 400-stream burst doesn't drain the acquire pool. */
    public static final Duration DEFAULT_CONNECTION_ACQUIRE_TIMEOUT = Duration.ofSeconds(60);

    /** Time to wait for the AWS SDK to subscribe to the bidi-stream input publisher before failing. */
    public static final Duration DEFAULT_SUBSCRIPTION_TIMEOUT = Duration.ofSeconds(60);

    /**
     * Max simultaneous in-flight HTTP/2 streams across the shared Netty connection
     * pool. Combined with {@code maxStreams=1} (set by {@link SageMakerTransportFactory}),
     * this is the cap on simultaneous bidirectional streams.
     */
    public static final int DEFAULT_MAX_CONCURRENCY = 500;

    /** Max retries on transient AWS errors (throttling, pool-exhausted, transient connect) per stream. */
    public static final int DEFAULT_MAX_RETRIES = 5;

    /** First backoff delay after the initial failure. */
    public static final Duration DEFAULT_INITIAL_BACKOFF = Duration.ofMillis(100);

    /** Cap on the per-attempt backoff delay regardless of multiplier. */
    public static final Duration DEFAULT_MAX_BACKOFF = Duration.ofSeconds(5);

    /** Exponential growth factor applied between retry attempts. */
    public static final double DEFAULT_BACKOFF_MULTIPLIER = 2.0;

    /** Total wall-clock budget across all retry attempts before giving up and surfacing the error. */
    public static final Duration DEFAULT_RETRY_BUDGET = Duration.ofSeconds(30);

    private final String endpointName;
    private final Region region;
    private final String contentType;
    private final String acceptType;
    private final Duration connectionTimeout;
    private final Duration connectionAcquireTimeout;
    private final Duration subscriptionTimeout;
    private final int maxConcurrency;
    private final int maxRetries;
    private final Duration initialBackoff;
    private final Duration maxBackoff;
    private final double backoffMultiplier;
    private final Duration retryBudget;

    private SageMakerConfig(Builder builder) {
        this.endpointName = builder.endpointName;
        this.region = builder.region;
        this.contentType = builder.contentType;
        this.acceptType = builder.acceptType;
        this.connectionTimeout = builder.connectionTimeout;
        this.connectionAcquireTimeout = builder.connectionAcquireTimeout;
        this.subscriptionTimeout = builder.subscriptionTimeout;
        this.maxConcurrency = builder.maxConcurrency;
        this.maxRetries = builder.maxRetries;
        this.initialBackoff = builder.initialBackoff;
        this.maxBackoff = builder.maxBackoff;
        this.backoffMultiplier = builder.backoffMultiplier;
        this.retryBudget = builder.retryBudget;
    }

    public String endpointName() { return endpointName; }
    public Region region() { return region; }
    public String contentType() { return contentType; }
    public String acceptType() { return acceptType; }
    public Duration connectionTimeout() { return connectionTimeout; }
    public Duration connectionAcquireTimeout() { return connectionAcquireTimeout; }
    public Duration subscriptionTimeout() { return subscriptionTimeout; }
    public int maxConcurrency() { return maxConcurrency; }
    public int maxRetries() { return maxRetries; }
    public Duration initialBackoff() { return initialBackoff; }
    public Duration maxBackoff() { return maxBackoff; }
    public double backoffMultiplier() { return backoffMultiplier; }
    public Duration retryBudget() { return retryBudget; }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String endpointName;
        private Region region = Region.US_WEST_2;
        private String contentType = "application/octet-stream";
        private String acceptType = "application/json";
        private Duration connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
        private Duration connectionAcquireTimeout = DEFAULT_CONNECTION_ACQUIRE_TIMEOUT;
        private Duration subscriptionTimeout = DEFAULT_SUBSCRIPTION_TIMEOUT;
        private int maxConcurrency = DEFAULT_MAX_CONCURRENCY;
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private Duration initialBackoff = DEFAULT_INITIAL_BACKOFF;
        private Duration maxBackoff = DEFAULT_MAX_BACKOFF;
        private double backoffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;
        private Duration retryBudget = DEFAULT_RETRY_BUDGET;

        public Builder endpointName(String endpointName) {
            this.endpointName = endpointName;
            return this;
        }

        public Builder region(Region region) {
            this.region = region;
            return this;
        }

        public Builder region(String region) {
            this.region = Region.of(region);
            return this;
        }

        public Builder contentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        public Builder acceptType(String acceptType) {
            this.acceptType = acceptType;
            return this;
        }

        /**
         * Max time to wait for the underlying TCP/TLS connect to complete.
         * Forwards to {@code NettyNioAsyncHttpClient.Builder.connectionTimeout}.
         */
        public Builder connectionTimeout(Duration connectionTimeout) {
            if (connectionTimeout == null || connectionTimeout.isNegative() || connectionTimeout.isZero()) {
                throw new IllegalArgumentException("connectionTimeout must be positive");
            }
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        /**
         * Max time to wait when acquiring a connection from the Netty pool.
         * Forwards to {@code NettyNioAsyncHttpClient.Builder.connectionAcquisitionTimeout}.
         */
        public Builder connectionAcquireTimeout(Duration connectionAcquireTimeout) {
            if (connectionAcquireTimeout == null
                    || connectionAcquireTimeout.isNegative()
                    || connectionAcquireTimeout.isZero()) {
                throw new IllegalArgumentException("connectionAcquireTimeout must be positive");
            }
            this.connectionAcquireTimeout = connectionAcquireTimeout;
            return this;
        }

        /**
         * Max time the transport waits for the AWS SDK to subscribe to the
         * bidirectional input publisher before failing the first send.
         */
        public Builder subscriptionTimeout(Duration subscriptionTimeout) {
            if (subscriptionTimeout == null
                    || subscriptionTimeout.isNegative()
                    || subscriptionTimeout.isZero()) {
                throw new IllegalArgumentException("subscriptionTimeout must be positive");
            }
            this.subscriptionTimeout = subscriptionTimeout;
            return this;
        }

        /**
         * Max simultaneous in-flight HTTP/2 streams across the shared Netty pool.
         * With {@code maxStreams=1} this equals the maximum number of concurrent
         * bidirectional streams the factory can support.
         */
        public Builder maxConcurrency(int maxConcurrency) {
            if (maxConcurrency <= 0) {
                throw new IllegalArgumentException("maxConcurrency must be positive");
            }
            this.maxConcurrency = maxConcurrency;
            return this;
        }

        /**
         * Max retries on transient AWS errors per stream invocation. Set to {@code 0} to disable
         * internal retry. Transient errors include throttling, connection-pool exhaustion, and
         * transient connect/timeout failures; terminal errors (auth, validation) bypass this and
         * surface to the application immediately.
         */
        public Builder maxRetries(int maxRetries) {
            if (maxRetries < 0) {
                throw new IllegalArgumentException("maxRetries must be non-negative");
            }
            this.maxRetries = maxRetries;
            return this;
        }

        /** First backoff delay applied after the initial failure. */
        public Builder initialBackoff(Duration initialBackoff) {
            if (initialBackoff == null || initialBackoff.isNegative() || initialBackoff.isZero()) {
                throw new IllegalArgumentException("initialBackoff must be positive");
            }
            this.initialBackoff = initialBackoff;
            return this;
        }

        /** Cap on the per-attempt backoff delay regardless of multiplier. */
        public Builder maxBackoff(Duration maxBackoff) {
            if (maxBackoff == null || maxBackoff.isNegative() || maxBackoff.isZero()) {
                throw new IllegalArgumentException("maxBackoff must be positive");
            }
            this.maxBackoff = maxBackoff;
            return this;
        }

        /** Exponential growth factor applied between retry attempts. Must be {@code >= 1.0}. */
        public Builder backoffMultiplier(double backoffMultiplier) {
            if (backoffMultiplier < 1.0) {
                throw new IllegalArgumentException("backoffMultiplier must be >= 1.0");
            }
            this.backoffMultiplier = backoffMultiplier;
            return this;
        }

        /** Total wall-clock budget across all retry attempts before giving up. */
        public Builder retryBudget(Duration retryBudget) {
            if (retryBudget == null || retryBudget.isNegative() || retryBudget.isZero()) {
                throw new IllegalArgumentException("retryBudget must be positive");
            }
            this.retryBudget = retryBudget;
            return this;
        }

        public SageMakerConfig build() {
            if (endpointName == null || endpointName.isBlank()) {
                throw new IllegalArgumentException("endpointName is required");
            }
            if (initialBackoff.compareTo(maxBackoff) > 0) {
                throw new IllegalArgumentException("initialBackoff (" + initialBackoff
                        + ") must not exceed maxBackoff (" + maxBackoff + ")");
            }
            return new SageMakerConfig(this);
        }
    }
}
