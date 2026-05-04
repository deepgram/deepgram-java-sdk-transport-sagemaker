package com.deepgram.sagemaker;

import com.deepgram.core.transport.DeepgramTransport;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger log = LoggerFactory.getLogger(SageMakerTransport.class);
    private final String transportId = String.format("%08x", System.identityHashCode(this));

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

    // Replay buffer: events sent on the current stream that AWS hasn't acked yet (no payload part
    // received since they were sent). On internal reset (handleStreamError → RETRYABLE), the next
    // attemptConnect drains this buffer onto the new stream so audio sent on the rejected stream
    // isn't lost. Trimmed in handlePayloadPart (a transcript proves AWS has consumed prior audio)
    // and capped at config.maxReplayBufferBytes() with FIFO eviction so unbounded throttle storms
    // don't OOM the JVM.
    private final ConcurrentLinkedDeque<BufferedEvent> replayBuffer = new ConcurrentLinkedDeque<>();
    private final AtomicLong replayBufferBytes = new AtomicLong(0L);

    // Retry budget tracking. Reset to 0 once real downstream data flows back to the application
    // (handlePayloadPart) — NOT on subscription success. Subscription succeeds in TLS+HTTP/2
    // setup terms even when the bidi-stream request will be throttled milliseconds later.
    private final AtomicInteger retryAttempt = new AtomicInteger(0);
    private volatile long retryWindowStart = 0L;
    // Earliest wall-clock at which the next attemptConnect is allowed to proceed. Set by
    // handleStreamError so post-subscription throttles (which never reach the ensureConnected
    // catch-block backoff) still pace the next attempt.
    private volatile long retryNotBeforeMs = 0L;

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
        // Fast path requires BOTH connected=true AND a live publisher. If either is false we
        // re-enter the synchronized reconnect loop. Without the publisher liveness check, a
        // race between handleStreamError (which sets connected=false then completes the
        // publisher) and ensureConnected's tail (connected.set(true)) can leave us with
        // connected=true pointing at a dead publisher — sends silently NOOP forever and the
        // conn never recovers (root cause of the 44 silent conns observed in the
        // 2026-05-03 replay-buffer-only test).
        StreamPublisher pub = inputPublisher;
        if (connected.get() && pub != null && !pub.isCompleted()) return;
        synchronized (connectLock) {
            pub = inputPublisher;
            if (connected.get() && pub != null && !pub.isCompleted()) return;
            // If we got here because the publisher died but connected was still true (the race
            // above), tell the loop to start a fresh attempt rather than treat ourselves as
            // already-connected.
            connected.set(false);

            if (retryWindowStart == 0L) {
                retryWindowStart = System.currentTimeMillis();
            }

            Throwable lastError = null;
            while (true) {
                // Honor backoff scheduled by handleStreamError so post-subscription throttles
                // pace the next attempt (the catch-block backoff below never fires for those —
                // attemptConnect succeeds, the throttle hits later).
                long now = System.currentTimeMillis();
                if (retryNotBeforeMs > now) {
                    long sleepMs = retryNotBeforeMs - now;
                    log.info("[{}] ensureConnected: honoring scheduled backoff ({}ms) before next attemptConnect",
                            transportId, sleepMs);
                    try {
                        Thread.sleep(sleepMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during scheduled backoff", lastError);
                    }
                }
                int attemptBefore = retryAttempt.get();
                log.info("[{}] ensureConnected: starting attemptConnect (attempt={}/{}, elapsed={}ms/{}ms)",
                        transportId, attemptBefore, config.maxRetries(),
                        System.currentTimeMillis() - retryWindowStart, config.retryBudget().toMillis());
                try {
                    attemptConnect();
                    // Subscription succeeded — but DO NOT reset retryAttempt here. Subscription
                    // success only proves TLS+HTTP/2 setup; the actual bidi-stream request can
                    // still be throttled. We reset retryAttempt only when real downstream data
                    // arrives, in handlePayloadPart.
                    log.info("[{}] ensureConnected: attemptConnect SUCCEEDED (attempt={} — counter NOT reset; "
                            + "waits for handlePayloadPart to confirm real data flow before resetting)",
                            transportId, attemptBefore);
                    connected.set(true);
                    return;
                } catch (Throwable t) {
                    lastError = t;
                    Classification c = classify(t);
                    int attempt = retryAttempt.get();
                    long elapsed = System.currentTimeMillis() - retryWindowStart;
                    boolean budgetLeft = attempt < config.maxRetries()
                            && elapsed < config.retryBudget().toMillis();
                    log.info("[{}] ensureConnected: attemptConnect FAILED — class={} attempt={}/{} elapsed={}ms/{}ms budgetLeft={} err={}",
                            transportId, c, attempt, config.maxRetries(), elapsed,
                            config.retryBudget().toMillis(), budgetLeft, summarize(t));
                    if (c == Classification.TERMINAL || !budgetLeft) {
                        log.warn("[{}] ensureConnected: SURFACING (class={} budgetLeft={}) — err={}",
                                transportId, c, budgetLeft, summarize(t));
                        if (t instanceof RuntimeException) throw (RuntimeException) t;
                        throw new RuntimeException(t);
                    }
                    long backoff = computeBackoff(attempt);
                    log.info("[{}] ensureConnected: backoff={}ms before retry attempt {}",
                            transportId, backoff, attempt + 1);
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

        // Replay any unacked events from a prior failed stream attempt. They go through the
        // publisher's pre-subscription pending queue and are flushed onto the new AWS subscriber
        // as soon as it arrives — so AWS sees the buffered audio first, in order, before any
        // new chunks the caller sends after this attempt succeeds.
        if (!replayBuffer.isEmpty()) {
            int count = replayBuffer.size();
            long bytes = replayBufferBytes.get();
            log.info("[{}] attemptConnect: replaying {} buffered events ({} bytes) onto new stream",
                    transportId, count, bytes);
            for (BufferedEvent be : replayBuffer) {
                publisher.send(be.event);
            }
        }

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

        // Race guard: under high-concurrency burst-throttle, AWS sometimes accepts the
        // subscription and IMMEDIATELY rejects the stream (ThrottlingException, ping-timeout).
        // The handleStreamError callback fires on a Netty event-loop thread BEFORE attemptConnect
        // returns from awaitSubscription, sets connected=false, and completes this publisher.
        // If we then unconditionally let ensureConnected set connected=true, the audio thread
        // takes the fast path and pushes chunks into a dead publisher — silently dropped, no
        // transcripts ever come back. Detect the race here by re-checking the publisher's
        // completion state and surfacing it as a transient failure so ensureConnected re-enters
        // the retry loop on the next send.
        if (publisher.isCompleted()) {
            throw new IllegalStateException(
                    "Stream rejected immediately after subscription (handleStreamError already "
                  + "fired and completed the publisher); ensureConnected will re-attempt");
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
            log.info("[{}] handleStreamError: closeSent=true → treating as normal close", transportId);
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

        log.info("[{}] handleStreamError: class={} attempt={}/{} elapsed={}ms/{}ms budgetLeft={} err={}",
                transportId, c, attempt, config.maxRetries(), elapsed,
                config.retryBudget().toMillis(), budgetLeft, summarize(error));

        if (c == Classification.RETRYABLE && budgetLeft) {
            // Internal reset: drop current stream, mark disconnected, and SCHEDULE a backoff so
            // the next ensureConnected pause-then-reconnect rather than immediately hammering
            // the AWS frontline. We don't sleep here (this runs on a Netty event-loop thread);
            // we just set retryNotBeforeMs and the next ensureConnected honors it.
            //
            // We also advance retryAttempt and start the budget window if not already started,
            // so under repeated post-subscription throttles the retry budget actually gets
            // consumed and eventually surfaces a terminal error to the application.
            if (retryWindowStart == 0L) {
                retryWindowStart = System.currentTimeMillis();
            }
            int attemptForBackoff = retryAttempt.getAndIncrement();
            long backoff = computeBackoff(attemptForBackoff);
            retryNotBeforeMs = System.currentTimeMillis() + backoff;
            log.info("[{}] handleStreamError: RETRYABLE → internal reset, attempt {} → scheduled backoff {}ms",
                    transportId, attemptForBackoff + 1, backoff);
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
        log.warn("[{}] handleStreamError: SURFACING (class={} budgetLeft={}) → invoking {} errorListener(s)",
                transportId, c, budgetLeft, errorListeners.size());
        for (Consumer<Throwable> l : errorListeners) {
            l.accept(error);
        }
    }

    /** One-line summary of a Throwable for log lines. */
    private static String summarize(Throwable t) {
        if (t == null) return "null";
        String msg = t.getMessage();
        if (msg == null) msg = "";
        if (msg.length() > 160) msg = msg.substring(0, 157) + "...";
        StringBuilder sb = new StringBuilder(t.getClass().getSimpleName()).append(": ").append(msg);
        if (t instanceof AwsServiceException) {
            AwsServiceException ase = (AwsServiceException) t;
            sb.append(" [status=").append(ase.statusCode());
            if (ase.awsErrorDetails() != null && ase.awsErrorDetails().errorCode() != null) {
                sb.append(" code=").append(ase.awsErrorDetails().errorCode());
            }
            sb.append("]");
        }
        return sb.toString();
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
     * (surface to {@code errorListeners}).
     *
     * <p>Default is RETRYABLE — the retry budget ({@link SageMakerConfig#retryBudget()}) is the
     * safety net, not the classifier. Under high-burst load against SageMaker we've seen a
     * long tail of transient failure modes (Netty WriteTimeout, HTTP/2 stream resets, SSO 429s,
     * pool-acquire timeouts, AWS-frontline ThrottlingException) that the classifier kept missing
     * one-by-one; flipping the default means a new transient hiccup retries instead of surfacing
     * as a hard failure. The budget caps the worst case at the configured wall-clock anyway.
     *
     * <p>The narrow set we genuinely consider TERMINAL is caller-side rejections from AWS:
     * an {@link AwsServiceException} with a 4xx status code (other than 429, 424, and other than
     * "throttling"-coded errors). Validation errors, AccessDenied, ResourceNotFound, etc. are
     * authoritative — retrying won't change the outcome. Everything else, including the cause
     * chain we don't recognize, defaults to RETRYABLE.
     *
     * <p>424 (Failed Dependency) is treated as RETRYABLE because under burst load we observed
     * SageMaker emitting {@code ModelErrorException: Received server error (424) from primary
     * with message "Failed to establish WebSocket connection"} when the upstream model container
     * couldn't accept a new stream at the moment of the request. The next attempt usually
     * succeeds, so it doesn't belong in the caller-side-rejection bucket.
     */
    static Classification classify(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof AwsServiceException) {
                AwsServiceException ase = (AwsServiceException) t;
                int status = ase.statusCode();
                String code = ase.awsErrorDetails() != null ? ase.awsErrorDetails().errorCode() : null;
                // Throttling is sometimes coded as 4xx (e.g. SageMaker frontline returns 400 with
                // errorCode=ThrottlingException); always retry these regardless of status.
                if (code != null && code.toLowerCase().contains("throttl")) return Classification.RETRYABLE;
                // 4xx caller-side rejections: validation, auth, notfound — won't fix on retry.
                // Exclusions: 429 (rate-limit, retry-with-backoff) and 424 (Failed Dependency,
                // SageMaker upstream "Failed to establish WebSocket connection" — transient).
                if (status >= 400 && status < 500 && status != 429 && status != 424) {
                    return Classification.TERMINAL;
                }
            }
            if (t == t.getCause()) break;
        }
        return Classification.RETRYABLE;
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
        bufferForReplay(event, data.length);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> sendText(String data) {
        if (!open.get()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Transport is closed"));
        }
        ensureConnected();
        byte[] payload = data.getBytes(StandardCharsets.UTF_8);
        RequestStreamEvent event = RequestStreamEvent.payloadPartBuilder()
                .bytes(SdkBytes.fromByteArray(payload))
                .dataType("UTF8")
                .build();
        inputPublisher.send(event);
        bufferForReplay(event, payload.length);

        // Track that we've signaled end-of-audio so we can treat the model's
        // idle timeout as a normal close rather than an error.
        if (data.contains("\"type\":\"CloseStream\"") || data.contains("\"type\":\"Finalize\"")) {
            closeSent.set(true);
        }

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Append {@code event} to the replay buffer for re-delivery on a future internal reset.
     * Evicts oldest events FIFO once the configured byte cap is exceeded so an unbounded
     * throttle storm can't OOM the JVM. Caller passes the payload byte length so we don't
     * have to re-walk the SdkBytes wrapper (which would double-allocate on every send).
     */
    private void bufferForReplay(RequestStreamEvent event, int eventBytes) {
        long cap = config.maxReplayBufferBytes();
        if (cap == 0L) return;  // replay disabled
        replayBuffer.addLast(new BufferedEvent(event, eventBytes));
        long total = replayBufferBytes.addAndGet(eventBytes);
        while (total > cap) {
            BufferedEvent dropped = replayBuffer.pollFirst();
            if (dropped == null) break;
            total = replayBufferBytes.addAndGet(-dropped.bytes);
        }
    }

    /** Drain the replay buffer (e.g. after AWS acks via {@code handlePayloadPart}). */
    private void clearReplayBuffer() {
        replayBuffer.clear();
        replayBufferBytes.set(0L);
    }

    // Test-only accessors. Package-private so unit tests in the same package can verify buffer
    // behavior without going through the full AWS reactive-streams stack.
    int replayBufferSize() { return replayBuffer.size(); }
    long replayBufferBytes() { return replayBufferBytes.get(); }
    void bufferForReplayForTest(RequestStreamEvent event, int eventBytes) {
        bufferForReplay(event, eventBytes);
    }
    void clearReplayBufferForTest() { clearReplayBuffer(); }
    java.util.List<RequestStreamEvent> drainReplayBufferForTest() {
        java.util.List<RequestStreamEvent> out = new java.util.ArrayList<>(replayBuffer.size());
        for (BufferedEvent be : replayBuffer) out.add(be.event);
        return out;
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

        // Decode once. JSON messages start with '{"' (0x7B 0x22) — checking two bytes avoids
        // false positives from binary audio chunks that happen to start with 0x7B.
        boolean isJson = bytes.length > 1 && bytes[0] == '{' && bytes[1] == '"';
        String text = isJson ? new String(bytes, StandardCharsets.UTF_8) : null;

        // Decide whether this payload counts as "the model consumed input" — the signal that
        // makes it safe to reset retry counters and trim the replay buffer.
        //
        // Rather than enumerate every downstream message type per Deepgram product (Listen STT
        // emits `Results`, Flux emits `TurnInfo`, Speak/Aura emits binary audio chunks, ...),
        // we invert the check: assume ANY downstream payload counts as a real ack EXCEPT for
        // end-of-stream-only message types that the container can emit without having consumed
        // input. Two types are documented to fire at close regardless of consumption:
        //
        //   - `Metadata` — end-of-stream summary. Under burst-replay we observed the container
        //     emitting `Metadata` (sha256="incomplete", duration=0) when the model errored out
        //     before producing any transcript; the original code treated this as an ack and
        //     cleared the replay buffer, so the next retry had nothing to replay and the
        //     replayed audio was lost (root cause of conn 125-style front-loss in the
        //     2026-05-03 RC run).
        //   - `Error` — model-side fatal error message, also typically sent at close.
        //
        // Anything else — Results, TurnInfo, EndOfTurn, Warning, SpeechStarted, UtteranceEnd,
        // raw binary audio (TTS) — represents real model activity and is safe to trust.
        boolean isCloseOnlyMessage = isJson
                && (text.contains("\"type\":\"Metadata\"") || text.contains("\"type\":\"Error\""));
        boolean countsAsAck = !isCloseOnlyMessage;

        if (countsAsAck) {
            // Real downstream content on this transport (or first since the last retry-loop
            // reset) → the stream is genuinely working end-to-end. Reset the retry budget so a
            // future transient failure gets a fresh budget.
            if (retryAttempt.get() != 0 || retryWindowStart != 0L || retryNotBeforeMs != 0L) {
                log.info("[{}] handlePayloadPart: ack-class data received ({}B) → resetting retry counters "
                        + "(was attempt={}, windowStart={}, notBeforeMs={})",
                        transportId, bytes.length,
                        retryAttempt.get(), retryWindowStart, retryNotBeforeMs);
                retryAttempt.set(0);
                retryWindowStart = 0L;
                retryNotBeforeMs = 0L;
            }

            // The model produced output → it consumed input up to (at least) the moment this
            // part's window started. Drop the replay buffer; if a retry happens later, we only
            // need to replay events sent SINCE this ack. The model's internal latency means a
            // small leading window (< 1 s, typically) of audio sent just before the failure
            // might be re-transcribed on the new stream — net impact is a brief duplicate, far
            // better than dropping 30+ s of audio outright.
            clearReplayBuffer();
        }

        if (isJson) {
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
        // Terminal close — drop any messages that were queued during a reset window, and free the
        // replay buffer so we don't pin its memory after the transport is done.
        pending.clear();
        clearReplayBuffer();
        if (inputPublisher != null) {
            inputPublisher.complete();
        }
        if (streamFuture != null) {
            streamFuture.cancel(true);
        }
    }

    /**
     * Replay-buffer entry — keeps the byte length alongside the event so the cap-and-evict path
     * doesn't have to peek inside SdkBytes (which would incur an extra copy on every send).
     */
    static final class BufferedEvent {
        final RequestStreamEvent event;
        final int bytes;

        BufferedEvent(RequestStreamEvent event, int bytes) {
            this.event = event;
            this.bytes = bytes;
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

        /**
         * @return {@code true} if {@link #complete()} or the subscriber's cancel hook has fired.
         *         Used by the {@code attemptConnect} race-guard to detect when handleStreamError
         *         has already torn down the publisher between subscription and the
         *         {@code connected.set(true)} fast-path.
         */
        boolean isCompleted() {
            return completed.get();
        }
    }
}
