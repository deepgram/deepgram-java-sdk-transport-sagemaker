package com.deepgram.sagemaker;

import com.deepgram.core.transport.DeepgramTransport;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sagemakerruntimehttp2.SageMakerRuntimeHttp2AsyncClient;
import software.amazon.awssdk.services.sagemakerruntimehttp2.model.InvokeEndpointWithBidirectionalStreamRequest;
import software.amazon.awssdk.services.sagemakerruntimehttp2.model.InvokeEndpointWithBidirectionalStreamResponseHandler;
import software.amazon.awssdk.services.sagemakerruntimehttp2.model.RequestStreamEvent;
import software.amazon.awssdk.services.sagemakerruntimehttp2.model.ResponsePayloadPart;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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

    // Hoisted out of StreamPublisher so messages queued during an internal reset survive into the
    // next stream attempt instead of being dropped with the discarded publisher.
    private final ConcurrentLinkedQueue<RequestStreamEvent> pending = new ConcurrentLinkedQueue<>();

    // Retry budget tracking. Reset to 0 once a stream successfully establishes (subscription).
    private final AtomicInteger retryAttempt = new AtomicInteger(0);
    private volatile long retryWindowStart = 0L;

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
     * Establish the bidirectional stream if not already connected. Blocks until the AWS SDK
     * subscribes to the event publisher.
     *
     * <p>Internally retries with exponential backoff on transient AWS errors (throttling,
     * connection-pool exhaustion, transient connect/timeout failures) bounded by
     * {@link SageMakerConfig#maxRetries()} and {@link SageMakerConfig#retryBudget()}. Terminal
     * errors (auth, validation) and budget exhaustion bubble out and surface to {@code errorListeners}
     * via the caller's {@code send*} path.
     */
    private void ensureConnected() {
        if (connected.get()) return;
        synchronized (connectLock) {
            if (connected.get()) return;

            if (retryWindowStart == 0L) {
                retryWindowStart = System.currentTimeMillis();
            }

            Throwable lastError = null;
            while (true) {
                try {
                    attemptConnect();
                    // Success: reset retry budget for any future internal reconnects on this transport.
                    retryAttempt.set(0);
                    retryWindowStart = 0L;
                    connected.set(true);
                    return;
                } catch (Throwable t) {
                    lastError = t;
                    Classification c = classify(t);
                    int attempt = retryAttempt.get();
                    long elapsed = System.currentTimeMillis() - retryWindowStart;
                    boolean budgetLeft = attempt < config.maxRetries()
                            && elapsed < config.retryBudget().toMillis();
                    if (c == Classification.TERMINAL || !budgetLeft) {
                        if (t instanceof RuntimeException) throw (RuntimeException) t;
                        throw new RuntimeException(t);
                    }
                    long backoff = computeBackoff(attempt);
                    retryAttempt.incrementAndGet();
                    try {
                        Thread.sleep(backoff);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(
                                "Interrupted during retry backoff after " + (attempt + 1) + " attempts", lastError);
                    }
                }
            }
        }
    }

    /** Single connect attempt — invokes the bidi stream and waits for subscription. */
    private void attemptConnect() throws TimeoutException, InterruptedException {
        StreamPublisher publisher = new StreamPublisher(pending);
        inputPublisher = publisher;

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
                        .onError(this::handleStreamError)
                        .onComplete(() -> notifyClose(1000, "Normal"))
                        .build();

        streamFuture = smClient.invokeEndpointWithBidirectionalStream(request, publisher, handler);

        if (!publisher.awaitSubscription(config.subscriptionTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
            // Subscription never landed — treat as a transient connect failure so the retry loop
            // classifies and (if budget allows) tries again on a fresh stream.
            try {
                streamFuture.cancel(true);
            } catch (Throwable ignored) {
                // best-effort
            }
            throw new TimeoutException(
                    "Timed out waiting for AWS SDK to subscribe to stream publisher after "
                            + config.subscriptionTimeout());
        }
    }

    /**
     * Async error gate: AWS SDK reports stream errors via the response handler's onError. Classify
     * here so transient AWS errors trigger an internal reset (next send re-enters ensureConnected
     * via the retry loop) instead of bubbling straight to {@code errorListeners}.
     */
    private void handleStreamError(Throwable error) {
        if (closeSent.get()) {
            // Model idle timeout after CloseStream — treat as normal close.
            if (inputPublisher != null) inputPublisher.complete();
            notifyClose(1000, "Normal");
            return;
        }

        Classification c = classify(error);
        int attempt = retryAttempt.get();
        long windowStart = retryWindowStart;
        long elapsed = windowStart == 0L ? 0L : System.currentTimeMillis() - windowStart;
        boolean budgetLeft = attempt < config.maxRetries()
                && elapsed < config.retryBudget().toMillis();

        if (c == Classification.RETRYABLE && budgetLeft) {
            // Internal reset: drop current stream, mark disconnected. Next send re-enters
            // ensureConnected → attemptConnect, which will drain `pending` into the new stream.
            connected.set(false);
            if (inputPublisher != null) inputPublisher.complete();
            if (streamFuture != null) {
                try {
                    streamFuture.cancel(true);
                } catch (Throwable ignored) {
                    // best-effort
                }
            }
            return;
        }

        // Terminal or budget-exhausted: surface to listeners.
        for (Consumer<Throwable> l : errorListeners) {
            l.accept(error);
        }
    }

    private long computeBackoff(int attempt) {
        long initial = config.initialBackoff().toMillis();
        long max = config.maxBackoff().toMillis();
        double scaled = initial * Math.pow(config.backoffMultiplier(), attempt);
        if (scaled > max || Double.isInfinite(scaled)) {
            return max;
        }
        return Math.max(initial, (long) scaled);
    }

    enum Classification { RETRYABLE, TERMINAL }

    /**
     * Classify an AWS-side exception as transient (retry internally, don't surface) vs terminal
     * (surface to {@code errorListeners}). Walks the cause chain so SDK-wrapped exceptions are
     * inspected too.
     */
    static Classification classify(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof TimeoutException) return Classification.RETRYABLE;
            if (t instanceof ConnectException) return Classification.RETRYABLE;
            if (t instanceof IOException) return Classification.RETRYABLE;
            if (t instanceof AwsServiceException) {
                AwsServiceException ase = (AwsServiceException) t;
                int status = ase.statusCode();
                if (status == 429 || (status >= 500 && status < 600)) return Classification.RETRYABLE;
                String code = ase.awsErrorDetails() != null ? ase.awsErrorDetails().errorCode() : null;
                if (code != null && code.toLowerCase().contains("throttl")) return Classification.RETRYABLE;
                return Classification.TERMINAL;
            }
            if (t instanceof SdkException) {
                String msg = t.getMessage() == null ? "" : t.getMessage().toLowerCase();
                if (msg.contains("acquire") || msg.contains("pool") || msg.contains("throttl")
                        || msg.contains("timeout")) {
                    return Classification.RETRYABLE;
                }
            }
            if (t == t.getCause()) break;
        }
        return Classification.TERMINAL;
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
                .dataType("UTF8")
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

        // JSON messages start with '{"' (0x7B 0x22). Checking two bytes avoids
        // false positives from binary audio chunks that happen to start with 0x7B.
        if (bytes.length > 1 && bytes[0] == '{' && bytes[1] == '"') {
            String text = new String(bytes, StandardCharsets.UTF_8);
            for (Consumer<String> l : messageListeners) {
                l.accept(text);
            }
        } else {
            for (Consumer<byte[]> l : binaryListeners) {
                l.accept(bytes);
            }
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
        // Terminal close — drop any messages that were queued during a reset window.
        pending.clear();
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
     *
     * <p>The {@code pending} queue is owned by the enclosing {@link SageMakerTransport} and
     * shared across reconnect cycles, so events queued during an internal reset are drained
     * onto whichever stream subscribes next.
     */
    static class StreamPublisher implements Publisher<RequestStreamEvent> {
        private volatile Subscriber<? super RequestStreamEvent> subscriber;
        private final AtomicBoolean completed = new AtomicBoolean(false);
        private final ConcurrentLinkedQueue<RequestStreamEvent> pending;
        private final CountDownLatch subscribed = new CountDownLatch(1);

        StreamPublisher(ConcurrentLinkedQueue<RequestStreamEvent> sharedPending) {
            this.pending = sharedPending;
        }

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
            // Flush any events that were queued before subscription (including events that
            // survived a previous internal reset).
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
                // Buffer until the SDK subscribes (this stream or the next one after a reset)
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

        /**
         * @return {@code true} if subscription happened within the timeout, {@code false} on timeout.
         */
        boolean awaitSubscription(long timeout, TimeUnit unit) throws InterruptedException {
            return subscribed.await(timeout, unit);
        }
    }
}
