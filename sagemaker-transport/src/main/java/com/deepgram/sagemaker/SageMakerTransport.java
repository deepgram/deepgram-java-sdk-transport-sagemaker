package com.deepgram.sagemaker;

import com.deepgram.core.transport.DeepgramTransport;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.sagemakerruntimehttp2.SageMakerRuntimeHttp2AsyncClient;
import software.amazon.awssdk.services.sagemakerruntimehttp2.model.InvokeEndpointWithBidirectionalStreamRequest;
import software.amazon.awssdk.services.sagemakerruntimehttp2.model.InvokeEndpointWithBidirectionalStreamResponseHandler;
import software.amazon.awssdk.services.sagemakerruntimehttp2.model.RequestStreamEvent;
import software.amazon.awssdk.services.sagemakerruntimehttp2.model.ResponsePayloadPart;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * {@link DeepgramTransport} implementation that routes Deepgram API calls through
 * a SageMaker endpoint using HTTP/2 bidirectional streaming.
 *
 * <p>Uses {@code InvokeEndpointWithBidirectionalStream} to send audio chunks
 * and receive transcript responses concurrently over a single HTTP/2 connection,
 * matching the behavior of the Python SageMaker transport.
 *
 * <h2>How it works</h2>
 * <ul>
 *   <li>{@code sendBinary(byte[])} — sends audio data immediately as a stream event</li>
 *   <li>{@code sendText(String)} — sends JSON control messages (KeepAlive, CloseStream, etc.)
 *       as stream events</li>
 *   <li>Response stream — each {@code PayloadPart} is delivered to message listeners</li>
 * </ul>
 */
public class SageMakerTransport implements DeepgramTransport {

    private final SageMakerRuntimeHttp2AsyncClient smClient;
    private final SageMakerConfig config;
    private final String invocationPath;
    private final String queryString;
    private final AtomicBoolean open = new AtomicBoolean(true);
    private final AtomicBoolean connected = new AtomicBoolean(false);

    private final List<Consumer<String>> messageListeners = new ArrayList<>();
    private final List<Consumer<byte[]>> binaryListeners = new ArrayList<>();
    private final List<Consumer<Throwable>> errorListeners = new ArrayList<>();
    private final List<CloseListener> closeListeners = new ArrayList<>();

    private final AtomicBoolean closeSent = new AtomicBoolean(false);
    private final AtomicBoolean closeNotified = new AtomicBoolean(false);
    private volatile StreamPublisher inputPublisher;
    private volatile CompletableFuture<Void> streamFuture;
    private final Object connectLock = new Object();

    SageMakerTransport(
            SageMakerRuntimeHttp2AsyncClient smClient,
            SageMakerConfig config,
            String invocationPath,
            String queryString) {
        this.smClient = smClient;
        this.config = config;
        this.invocationPath = invocationPath;
        this.queryString = queryString;
    }

    /**
     * Establish the bidirectional stream if not already connected.
     * Blocks until the SDK has subscribed to the event publisher.
     */
    private void ensureConnected() {
        if (connected.get()) return;
        synchronized (connectLock) {
            if (connected.get()) return;

            inputPublisher = new StreamPublisher();

            InvokeEndpointWithBidirectionalStreamRequest.Builder requestBuilder =
                    InvokeEndpointWithBidirectionalStreamRequest.builder()
                            .endpointName(config.endpointName())
                            .modelInvocationPath(invocationPath);
            if (queryString != null && !queryString.isEmpty()) {
                requestBuilder.modelQueryString(queryString);
            }
            InvokeEndpointWithBidirectionalStreamRequest request = requestBuilder.build();

            InvokeEndpointWithBidirectionalStreamResponseHandler handler =
                    InvokeEndpointWithBidirectionalStreamResponseHandler.builder()
                            .onResponse(response -> { })
                            .subscriber(InvokeEndpointWithBidirectionalStreamResponseHandler
                                    .Visitor.builder()
                                    .onPayloadPart(this::handlePayloadPart)
                                    .build())
                            .onError(error -> {
                                if (closeSent.get()) {
                                    // Model idle timeout after CloseStream — treat as normal close.
                                    inputPublisher.complete();
                                    notifyClose(1000, "Normal");
                                } else {
                                    for (Consumer<Throwable> l : errorListeners) {
                                        l.accept(error);
                                    }
                                }
                            })
                            .onComplete(() -> {
                                notifyClose(1000, "Normal");
                            })
                            .build();

            streamFuture = smClient.invokeEndpointWithBidirectionalStream(
                    request, inputPublisher, handler);

            // Wait for the SDK to subscribe to our publisher before sending events
            try {
                inputPublisher.awaitSubscription(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted waiting for stream subscription", e);
            }

            connected.set(true);
        }
    }

    @Override
    public CompletableFuture<Void> sendBinary(byte[] data) {
        if (!open.get()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Transport is closed"));
        }
        ensureConnected();
        RequestStreamEvent event = RequestStreamEvent.payloadPartBuilder()
                .bytes(SdkBytes.fromByteArray(data))
                .build();
        inputPublisher.send(event);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> sendText(String data) {
        if (!open.get()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Transport is closed"));
        }
        ensureConnected();
        RequestStreamEvent event = RequestStreamEvent.payloadPartBuilder()
                .bytes(SdkBytes.fromByteArray(data.getBytes(StandardCharsets.UTF_8)))
                .build();
        inputPublisher.send(event);

        // Track that we've signaled end-of-audio so we can treat the model's
        // idle timeout as a normal close rather than an error.
        if (data.contains("\"type\":\"CloseStream\"") || data.contains("\"type\":\"Finalize\"")) {
            closeSent.set(true);
        }

        return CompletableFuture.completedFuture(null);
    }

    private void notifyClose(int code, String reason) {
        if (closeNotified.compareAndSet(false, true)) {
            for (CloseListener l : closeListeners) {
                l.onClose(code, reason);
            }
        }
    }

    private void handlePayloadPart(ResponsePayloadPart part) {
        byte[] bytes = part.bytes().asByteArray();

        // Try to interpret as text (JSON transcript results)
        try {
            String text = new String(bytes, StandardCharsets.UTF_8);
            if (text.startsWith("{")) {
                for (Consumer<String> l : messageListeners) {
                    l.accept(text);
                }
                return;
            }
        } catch (Exception ignored) {
            // Not valid UTF-8 text — treat as binary
        }

        // Otherwise deliver as binary (audio data for TTS responses)
        for (Consumer<byte[]> l : binaryListeners) {
            l.accept(bytes);
        }
    }

    @Override
    public void onTextMessage(Consumer<String> listener) {
        messageListeners.add(listener);
    }

    @Override
    public void onBinaryMessage(Consumer<byte[]> listener) {
        binaryListeners.add(listener);
    }

    @Override
    public void onError(Consumer<Throwable> listener) {
        errorListeners.add(listener);
    }

    @Override
    public void onClose(CloseListener listener) {
        closeListeners.add(listener);
    }

    @Override
    public void onOpen(Runnable listener) {
        // Fire immediately — the actual HTTP/2 stream is established lazily on first send.
        listener.run();
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    @Override
    public void close() {
        if (!open.compareAndSet(true, false)) return;
        if (inputPublisher != null) {
            inputPublisher.complete();
        }
        if (streamFuture != null) {
            streamFuture.cancel(true);
        }
    }

    /**
     * Reactive Streams publisher that buffers events until the SDK subscribes,
     * then delivers them in order. After subscription, events are forwarded immediately.
     */
    static class StreamPublisher implements Publisher<RequestStreamEvent> {
        private volatile Subscriber<? super RequestStreamEvent> subscriber;
        private final AtomicBoolean completed = new AtomicBoolean(false);
        private final ConcurrentLinkedQueue<RequestStreamEvent> pending = new ConcurrentLinkedQueue<>();
        private final CountDownLatch subscribed = new CountDownLatch(1);

        @Override
        public void subscribe(Subscriber<? super RequestStreamEvent> s) {
            this.subscriber = s;
            s.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    // Events are pushed as they arrive via send()
                }

                @Override
                public void cancel() {
                    completed.set(true);
                }
            });
            // Flush any events that were queued before subscription
            RequestStreamEvent event;
            while ((event = pending.poll()) != null) {
                s.onNext(event);
            }
            subscribed.countDown();
        }

        void send(RequestStreamEvent event) {
            if (completed.get()) return;
            Subscriber<? super RequestStreamEvent> s = subscriber;
            if (s != null) {
                s.onNext(event);
            } else {
                // Buffer until the SDK subscribes
                pending.add(event);
            }
        }

        void complete() {
            if (completed.compareAndSet(false, true)) {
                Subscriber<? super RequestStreamEvent> s = subscriber;
                if (s != null) {
                    s.onComplete();
                }
            }
        }

        void awaitSubscription(long timeout, TimeUnit unit) throws InterruptedException {
            subscribed.await(timeout, unit);
        }
    }
}
