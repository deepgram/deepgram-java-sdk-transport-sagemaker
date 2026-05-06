package com.deepgram.sagemaker;

import com.deepgram.core.ReconnectingWebSocketListener;
import com.deepgram.core.transport.DeepgramTransport;
import com.deepgram.core.transport.DeepgramTransportFactory;

import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.nio.netty.Http2Configuration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.sagemakerruntimehttp2.SageMakerRuntimeHttp2AsyncClient;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory that creates SageMaker bidirectional streaming transports.
 *
 * <p>Parses the WebSocket URL provided by the Deepgram SDK to extract the
 * model invocation path (e.g. {@code v1/listen}) and query string
 * (e.g. {@code model=nova-3&language=en}), then creates a
 * {@link SageMakerTransport} that streams audio via HTTP/2.
 *
 * <p>Usage:
 * <pre>{@code
 * var factory = new SageMakerTransportFactory(
 *     SageMakerConfig.builder()
 *         .endpointName("my-deepgram-endpoint")
 *         .region("us-east-2")
 *         .build()
 * );
 *
 * var client = DeepgramClient.builder()
 *     .apiKey("unused")
 *     .transportFactory(factory)
 *     .build();
 * }</pre>
 *
 * <h2>Connection-pool sharing</h2>
 *
 * <p>The default constructor backs the factory with a <strong>process-wide shared</strong>
 * {@link SageMakerRuntimeHttp2AsyncClient} keyed by the parts of {@link SageMakerConfig} that
 * affect the underlying Netty HTTP/2 client (region, max concurrency, connect/acquire timeouts).
 * Multiple factories built with the same config fingerprint reuse one Netty event loop group and
 * one connection pool — so naive code that constructs a fresh factory per stream still gets a
 * single, well-behaved client underneath.
 *
 * <p>Without sharing, every factory instantiates its own Netty pool, and a burst of N factories
 * triggers N simultaneous TLS handshakes from N distinct Netty clients against the same SageMaker
 * endpoint — the SageMaker HTTP/2 frontline silently drops a large fraction of those streams
 * before they ever reach the model container. Sharing matches the behavior of the canonical
 * Python load-test harness, which has been verified to handle 400+ concurrent streams cleanly.
 *
 * <p>Lifecycle:
 * <ul>
 *   <li>Default constructor → shared client; {@link #shutdown()} is a no-op. Call
 *       {@link #shutdownAllSharedClients()} once at app shutdown to release Netty resources.
 *   <li>{@link #SageMakerTransportFactory(SageMakerConfig, SageMakerRuntimeHttp2AsyncClient)}
 *       (BYO client) → caller owns the client lifecycle; {@link #shutdown()} is a no-op.
 * </ul>
 */
public class SageMakerTransportFactory implements DeepgramTransportFactory {

    /**
     * Process-wide shared clients keyed by config fingerprint. Subsequent factories with the
     * same fingerprint reuse the existing client, so one Netty event loop group + connection
     * pool serves all of them.
     */
    private static final ConcurrentHashMap<String, SageMakerRuntimeHttp2AsyncClient> SHARED_CLIENTS =
            new ConcurrentHashMap<>();

    private final SageMakerConfig config;
    private final SageMakerRuntimeHttp2AsyncClient smClient;

    public SageMakerTransportFactory(SageMakerConfig config) {
        this.config = config;
        this.smClient = SHARED_CLIENTS.computeIfAbsent(
                sharedClientKey(config),
                k -> buildClient(config));
    }

    /**
     * Create with a pre-configured SageMaker HTTP/2 client (for testing or custom credential
     * providers). The provided client is <strong>not</strong> closed by {@link #shutdown()};
     * the caller owns its lifecycle.
     */
    public SageMakerTransportFactory(SageMakerConfig config, SageMakerRuntimeHttp2AsyncClient smClient) {
        this.config = config;
        this.smClient = smClient;
    }

    /**
     * Cache key for the shared-client pool. Includes only the fields that affect the underlying
     * Netty client; per-stream config (endpointName, contentType, retry knobs) doesn't.
     */
    private static String sharedClientKey(SageMakerConfig c) {
        return c.region().id()
                + "|" + c.maxConcurrency()
                + "|" + c.connectionTimeout().toMillis()
                + "|" + c.connectionAcquireTimeout().toMillis()
                + "|" + c.maxStreamsPerConnection()
                + "|" + (c.nettyEventLoopThreads() == null ? "default" : c.nettyEventLoopThreads())
                + "|" + (c.healthCheckPingPeriod() == null ? "default" : c.healthCheckPingPeriod().toMillis());
    }

    private static SageMakerRuntimeHttp2AsyncClient buildClient(SageMakerConfig config) {
        Http2Configuration.Builder http2Builder = Http2Configuration.builder()
                .maxStreams(config.maxStreamsPerConnection());
        if (config.healthCheckPingPeriod() != null) {
            http2Builder.healthCheckPingPeriod(config.healthCheckPingPeriod());
        }
        NettyNioAsyncHttpClient.Builder httpBuilder = NettyNioAsyncHttpClient.builder()
                .protocol(Protocol.HTTP2)
                .maxConcurrency(config.maxConcurrency())
                .connectionTimeout(config.connectionTimeout())
                .connectionAcquisitionTimeout(config.connectionAcquireTimeout())
                .http2Configuration(http2Builder.build());
        if (config.nettyEventLoopThreads() != null) {
            httpBuilder.eventLoopGroupBuilder(
                    software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup.builder()
                            .numberOfThreads(config.nettyEventLoopThreads())
            );
        }
        return SageMakerRuntimeHttp2AsyncClient.builder()
                .region(config.region())
                // Disable the AWS SDK's internal retry strategy. SageMakerTransport owns the
                // retry policy (handleStreamError + ensureConnected backoff). The AWS SDK's
                // default 3-attempt strategy compounds on top: every "1 retry" in our schedule
                // becomes ~4 hits on the SageMaker frontline (the original attempt + 3 SDK
                // retries with their own ~25/100/400 ms backoffs). Under a per-LB-IP throttle
                // ceiling measured in requests/sec, that amplification keeps the conn pinned
                // in the throttle window. AwsRetryStrategy.doNotRetry() removes the SDK layer;
                // transient TLS / connection-reset hiccups are still caught by our IOException
                // → RETRYABLE classification and the same backoff path.
                .overrideConfiguration(c -> c.retryStrategy(AwsRetryStrategy.doNotRetry()))
                .httpClientBuilder(httpBuilder)
                .build();
    }

    @Override
    public DeepgramTransport create(String url, Map<String, String> headers) {
        // Parse the WebSocket URL to extract invocation path and query string.
        // The SDK provides URLs like wss://api.deepgram.com/v1/listen?model=nova-3
        // Convert wss/ws to https/http so URI can parse it.
        String httpUrl = url.replace("wss://", "https://").replace("ws://", "http://");
        URI uri = URI.create(httpUrl);
        String invocationPath = uri.getPath() != null ? uri.getPath().replaceFirst("^/", "") : "";
        String queryString = uri.getQuery() != null ? uri.getQuery() : "";

        return new SageMakerTransport(smClient, config, invocationPath, queryString);
    }

    /**
     * Disable the SDK's wrapper-level reconnect loop. {@link SageMakerTransport} owns its own
     * retry/backoff/classification (see {@link SageMakerConfig#maxRetries()},
     * {@link SageMakerConfig#retryBudget()}); wrapping it in another retry layer compounds
     * transient AWS errors into Throttling-on-Throttling storms under burst load.
     */
    @Override
    public ReconnectingWebSocketListener.ReconnectOptions reconnectOptions() {
        return ReconnectingWebSocketListener.ReconnectOptions.builder()
                .maxRetries(0)
                .build();
    }

    /**
     * No-op for factories backed by the shared client pool or a caller-owned (BYO) client.
     * Use {@link #shutdownAllSharedClients()} to close shared clients at app shutdown; close
     * your own client directly if you provided one.
     */
    public void shutdown() {
        // Intentionally no-op. See class-level Javadoc for lifecycle semantics.
    }

    /**
     * Close all process-wide shared {@link SageMakerRuntimeHttp2AsyncClient} instances. Call
     * once at app shutdown if you want to release Netty resources cleanly before JVM exit.
     * Subsequent default-constructor factories will lazily build new shared clients.
     */
    public static void shutdownAllSharedClients() {
        for (SageMakerRuntimeHttp2AsyncClient client : SHARED_CLIENTS.values()) {
            try {
                client.close();
            } catch (Exception ignored) {
                // best-effort
            }
        }
        SHARED_CLIENTS.clear();
    }
}
