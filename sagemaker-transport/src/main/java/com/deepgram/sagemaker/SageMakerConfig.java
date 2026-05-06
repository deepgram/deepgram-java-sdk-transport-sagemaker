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

    /**
     * Cap on the in-memory replay buffer that holds sent-but-unacked stream events for the
     * current bidi stream attempt. If the SDK has to retry (throttling, post-subscription
     * stream reset), this buffer is drained onto the new stream so AWS sees a continuous
     * audio sequence instead of the gap created by the discarded events.
     *
     * <p>The buffer is trimmed when {@code handlePayloadPart} fires (the model just produced
     * a transcript, so prior audio is acked from our perspective), so under steady-state
     * operation it stays small (≤ a few hundred KB). It only grows during a throttle/reset
     * window where no payload parts come back. 8&nbsp;MiB ≈ 256&nbsp;s of 16&nbsp;kHz mono
     * 16-bit PCM, which covers the longest throttle storms we've seen in practice with
     * margin to spare. Lower the cap for tight memory budgets; raise it if you expect
     * longer retry windows per stream.
     */
    public static final long DEFAULT_MAX_REPLAY_BUFFER_BYTES = 8L * 1024 * 1024;

    /**
     * Max concurrent HTTP/2 streams multiplexed onto a single underlying TCP connection. Defaults
     * to 1, which gives each bidi stream its own dedicated TCP connection — preventing slow-stream
     * starvation but creating one HTTP/2 keep-alive ping cycle per logical stream. Under heavy
     * concurrent load (hundreds of simultaneous streams from one process), the resulting flood of
     * pings can saturate the Netty event-loop pool and trigger spurious {@code PingFailedException}
     * connection teardowns. Raise this (e.g. 50–200) to multiplex many streams onto fewer
     * connections and slash the ping load.
     */
    public static final long DEFAULT_MAX_STREAMS_PER_CONNECTION = 1L;

    /**
     * Number of Netty event-loop worker threads handling HTTP/2 frames for the shared client.
     * {@code null} (the default) lets the AWS SDK Netty client pick — currently {@code 2 * NCPU}.
     * Override when running large numbers of concurrent streams on hardware where the default
     * leaves event loops saturated by inbound transcript frames + ping ACK bookkeeping.
     */
    public static final Integer DEFAULT_NETTY_EVENT_LOOP_THREADS = null;

    /**
     * Period between HTTP/2 keep-alive PING frames sent to the server, and the timeout for the
     * PING ACK. {@code null} (the default) leaves the AWS SDK Netty client default in place
     * (currently 5&nbsp;s). Under heavy single-process load (hundreds of concurrent streams
     * sharing a small event-loop pool), 5 s is too tight: an event loop briefly busy processing
     * inbound transcript frames can fail to read the PING ACK in time, causing
     * {@code PingFailedException} → connection-death → cascading retries even though the
     * underlying connection is healthy. Bump to 30&nbsp;s+ to tolerate moderate event-loop
     * stalls; pass {@code Duration.ZERO} to disable PING frames entirely (the SDK's own retry
     * + stream-error path will still detect genuinely-dead connections).
     */
    public static final Duration DEFAULT_HEALTH_CHECK_PING_PERIOD = null;

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
    private final long maxReplayBufferBytes;
    private final long maxStreamsPerConnection;
    private final Integer nettyEventLoopThreads;
    private final Duration healthCheckPingPeriod;

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
        this.maxReplayBufferBytes = builder.maxReplayBufferBytes;
        this.maxStreamsPerConnection = builder.maxStreamsPerConnection;
        this.nettyEventLoopThreads = builder.nettyEventLoopThreads;
        this.healthCheckPingPeriod = builder.healthCheckPingPeriod;
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
    public long maxReplayBufferBytes() { return maxReplayBufferBytes; }
    public long maxStreamsPerConnection() { return maxStreamsPerConnection; }
    public Integer nettyEventLoopThreads() { return nettyEventLoopThreads; }
    public Duration healthCheckPingPeriod() { return healthCheckPingPeriod; }

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
        private long maxReplayBufferBytes = DEFAULT_MAX_REPLAY_BUFFER_BYTES;
        private long maxStreamsPerConnection = DEFAULT_MAX_STREAMS_PER_CONNECTION;
        private Integer nettyEventLoopThreads = DEFAULT_NETTY_EVENT_LOOP_THREADS;
        private Duration healthCheckPingPeriod = DEFAULT_HEALTH_CHECK_PING_PERIOD;

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

        /**
         * Cap on the in-memory replay buffer that holds sent-but-unacked stream events. Set to
         * {@code 0} to disable replay (sent events are dropped on internal reset, matching the
         * pre-replay-buffer behavior). See {@link #DEFAULT_MAX_REPLAY_BUFFER_BYTES}.
         */
        public Builder maxReplayBufferBytes(long maxReplayBufferBytes) {
            if (maxReplayBufferBytes < 0) {
                throw new IllegalArgumentException("maxReplayBufferBytes must be non-negative");
            }
            this.maxReplayBufferBytes = maxReplayBufferBytes;
            return this;
        }

        /**
         * Max concurrent HTTP/2 streams per underlying TCP connection. See
         * {@link #DEFAULT_MAX_STREAMS_PER_CONNECTION}. Raise above 1 to multiplex many bidi
         * streams onto fewer connections (slashes ping load); leave at 1 for one-stream-per-TCP
         * isolation.
         */
        public Builder maxStreamsPerConnection(long maxStreamsPerConnection) {
            if (maxStreamsPerConnection <= 0) {
                throw new IllegalArgumentException("maxStreamsPerConnection must be positive");
            }
            this.maxStreamsPerConnection = maxStreamsPerConnection;
            return this;
        }

        /**
         * Number of Netty event-loop worker threads. {@code null} (default) uses the AWS SDK's
         * default ({@code 2 * NCPU}). Override for high-burst single-process workloads where
         * the default leaves event loops saturated by inbound frame processing.
         */
        public Builder nettyEventLoopThreads(Integer nettyEventLoopThreads) {
            if (nettyEventLoopThreads != null && nettyEventLoopThreads <= 0) {
                throw new IllegalArgumentException("nettyEventLoopThreads must be positive or null");
            }
            this.nettyEventLoopThreads = nettyEventLoopThreads;
            return this;
        }

        /**
         * Period between HTTP/2 keep-alive PING frames (and ACK timeout). {@code null} (default)
         * uses the AWS SDK Netty client default (5&nbsp;s). Pass {@code Duration.ZERO} to disable
         * PING frames entirely. See {@link #DEFAULT_HEALTH_CHECK_PING_PERIOD}.
         */
        public Builder healthCheckPingPeriod(Duration healthCheckPingPeriod) {
            if (healthCheckPingPeriod != null && healthCheckPingPeriod.isNegative()) {
                throw new IllegalArgumentException("healthCheckPingPeriod must be non-negative or null");
            }
            this.healthCheckPingPeriod = healthCheckPingPeriod;
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
