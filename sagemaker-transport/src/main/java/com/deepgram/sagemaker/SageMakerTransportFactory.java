package com.deepgram.sagemaker;

import com.deepgram.core.transport.DeepgramTransport;
import com.deepgram.core.transport.DeepgramTransportFactory;

import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.nio.netty.Http2Configuration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.sagemakerruntimehttp2.SageMakerRuntimeHttp2AsyncClient;

import java.net.URI;
import java.util.Map;

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
 */
public class SageMakerTransportFactory implements DeepgramTransportFactory {

    private final SageMakerConfig config;
    private final SageMakerRuntimeHttp2AsyncClient smClient;

    /**
     * Default max concurrent HTTP/2 streams (in-flight requests) across the
     * shared connection pool.  With {@code maxStreams=1} each stream gets its
     * own TCP connection, so this value equals the maximum number of
     * simultaneous bidirectional streams the factory can support.
     */
    private static final int DEFAULT_MAX_CONCURRENCY = 500;

    public SageMakerTransportFactory(SageMakerConfig config) {
        this.config = config;
        this.smClient = SageMakerRuntimeHttp2AsyncClient.builder()
                .region(config.region())
                .httpClientBuilder(
                        NettyNioAsyncHttpClient.builder()
                                .protocol(Protocol.HTTP2)
                                .maxConcurrency(DEFAULT_MAX_CONCURRENCY)
                                .http2Configuration(
                                        Http2Configuration.builder()
                                                .maxStreams(1L)
                                                .build()
                                )
                )
                .build();
    }

    /**
     * Create with a pre-configured SageMaker HTTP/2 client (for testing or
     * custom credential providers).
     */
    public SageMakerTransportFactory(SageMakerConfig config, SageMakerRuntimeHttp2AsyncClient smClient) {
        this.config = config;
        this.smClient = smClient;
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

    /** Shut down the underlying AWS SDK client. */
    public void shutdown() {
        smClient.close();
    }
}
