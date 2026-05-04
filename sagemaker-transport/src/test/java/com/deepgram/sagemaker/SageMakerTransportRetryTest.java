package com.deepgram.sagemaker;

import static org.junit.jupiter.api.Assertions.*;

import com.deepgram.sagemaker.SageMakerTransport.StreamPublisher;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sagemakerruntimehttp2.model.RequestStreamEvent;

/**
 * Unit tests for {@link SageMakerTransport}'s retry/classification logic and the hoisted pending-queue
 * machinery. End-to-end retry against the AWS reactive-streams handler isn't covered here (the
 * handler indirection makes it hard to deterministically stub); those paths are exercised by the
 * burst test in the README.
 */
class SageMakerTransportRetryTest {

    // Helpers live on the outer class because @Nested inner classes can't have static members on Java 11.

    private static RequestStreamEvent payloadEvent(String s) {
        return RequestStreamEvent.payloadPartBuilder()
                .bytes(SdkBytes.fromUtf8String(s))
                .build();
    }

    private static Subscriber<RequestStreamEvent> noopSubscriber() {
        return new Subscriber<RequestStreamEvent>() {
            @Override
            public void onSubscribe(Subscription s) {}
            @Override
            public void onNext(RequestStreamEvent e) {}
            @Override
            public void onError(Throwable t) {}
            @Override
            public void onComplete() {}
        };
    }

    // RNG stubs for backoff tests — must live on outer class because @Nested inner classes can't
    // have static members on Java 11.
    private static final java.util.function.LongBinaryOperator MAX_RNG = (origin, bound) -> bound - 1;
    private static final java.util.function.LongBinaryOperator MIN_RNG = (origin, bound) -> origin;
    private static final java.util.function.LongBinaryOperator MID_RNG =
            (origin, bound) -> origin + (bound - origin) / 2;

    private static class CapturingSubscriber implements Subscriber<RequestStreamEvent> {
        final List<RequestStreamEvent> received = new ArrayList<>();
        final CountDownLatch completed = new CountDownLatch(1);

        @Override
        public void onSubscribe(Subscription s) {}
        @Override
        public void onNext(RequestStreamEvent e) {
            received.add(e);
        }
        @Override
        public void onError(Throwable t) {}
        @Override
        public void onComplete() {
            completed.countDown();
        }
    }

    @Nested
    @DisplayName("classify(Throwable)")
    class ClassifyTests {
        @Test
        @DisplayName("TimeoutException is retryable")
        void timeoutIsRetryable() {
            assertEquals(SageMakerTransport.Classification.RETRYABLE,
                    SageMakerTransport.classify(new TimeoutException("acquire timeout")));
        }

        @Test
        @DisplayName("ConnectException is retryable")
        void connectExceptionIsRetryable() {
            assertEquals(SageMakerTransport.Classification.RETRYABLE,
                    SageMakerTransport.classify(new ConnectException("connection refused")));
        }

        @Test
        @DisplayName("IOException is retryable")
        void ioExceptionIsRetryable() {
            assertEquals(SageMakerTransport.Classification.RETRYABLE,
                    SageMakerTransport.classify(new IOException("network error")));
        }

        @Test
        @DisplayName("CancellationException is retryable (covers self-induced retry-reset cancels)")
        void cancellationExceptionIsRetryable() {
            assertEquals(SageMakerTransport.Classification.RETRYABLE,
                    SageMakerTransport.classify(new CancellationException()));
        }

        @Test
        @DisplayName("FutureCancelledException-style wrapper (RuntimeException with CancellationException cause) is retryable")
        void cancellationWrappedInRuntimeIsRetryable() {
            // Mirrors AWS Netty's FutureCancelledException, which extends RuntimeException and
            // wraps a CancellationException as its cause.
            RuntimeException wrapper = new RuntimeException("future cancelled", new CancellationException());
            assertEquals(SageMakerTransport.Classification.RETRYABLE, SageMakerTransport.classify(wrapper));
        }

        @Test
        @DisplayName("AWS 429 (Too Many Requests) is retryable")
        void aws429IsRetryable() {
            AwsServiceException ase = AwsServiceException.builder()
                    .message("Rate exceeded")
                    .statusCode(429)
                    .build();
            assertEquals(SageMakerTransport.Classification.RETRYABLE, SageMakerTransport.classify(ase));
        }

        @Test
        @DisplayName("AWS 5xx is retryable")
        void aws5xxIsRetryable() {
            AwsServiceException ase = AwsServiceException.builder()
                    .message("internal")
                    .statusCode(503)
                    .build();
            assertEquals(SageMakerTransport.Classification.RETRYABLE, SageMakerTransport.classify(ase));
        }

        @Test
        @DisplayName("AWS error code containing 'throttl' is retryable regardless of status")
        void awsThrottlingErrorCodeIsRetryable() {
            AwsServiceException ase = AwsServiceException.builder()
                    .message("Rate exceeded")
                    .statusCode(400)
                    .awsErrorDetails(AwsErrorDetails.builder()
                            .errorCode("ThrottlingException")
                            .build())
                    .build();
            assertEquals(SageMakerTransport.Classification.RETRYABLE, SageMakerTransport.classify(ase));
        }

        @Test
        @DisplayName("AWS 401 (Unauthorized) is terminal")
        void aws401IsTerminal() {
            AwsServiceException ase = AwsServiceException.builder()
                    .message("Forbidden")
                    .statusCode(401)
                    .build();
            assertEquals(SageMakerTransport.Classification.TERMINAL, SageMakerTransport.classify(ase));
        }

        @Test
        @DisplayName("AWS 403 (Forbidden) is terminal")
        void aws403IsTerminal() {
            AwsServiceException ase = AwsServiceException.builder()
                    .message("Forbidden")
                    .statusCode(403)
                    .build();
            assertEquals(SageMakerTransport.Classification.TERMINAL, SageMakerTransport.classify(ase));
        }

        @Test
        @DisplayName("SdkException with 'pool exhausted' message is retryable (defensive belt)")
        void sdkExceptionWithPoolKeywordIsRetryable() {
            SdkException sdke = SdkException.builder()
                    .message("Connection pool exhausted")
                    .build();
            assertEquals(SageMakerTransport.Classification.RETRYABLE, SageMakerTransport.classify(sdke));
        }

        @Test
        @DisplayName("'Unable to load credentials' is retryable when an SSO/STS provider hit Status Code: 429")
        void credentialLoadFailureWithSsoThrottleIsRetryable() {
            SdkException sdke = SdkException.builder()
                    .message("Unable to load credentials from any of the providers in the chain "
                            + "AwsCredentialsProviderChain(...): [..., "
                            + "ProfileCredentialsProvider(profileName=shared-dev, ...): "
                            + "HTTP 429 Unknown Code (Service: Sso, Status Code: 429, Request ID: abc) "
                            + "(SDK Attempt Count: 4), ...]")
                    .build();
            assertEquals(SageMakerTransport.Classification.RETRYABLE,
                    SageMakerTransport.classify(sdke));
        }

        @Test
        @DisplayName("'Unable to load credentials' is retryable when a credential backend returned 5xx")
        void credentialLoadFailureWith5xxIsRetryable() {
            SdkException sdke = SdkException.builder()
                    .message("Unable to load credentials from any of the providers in the chain "
                            + "AwsCredentialsProviderChain(...): [..., "
                            + "InstanceProfileCredentialsProvider(): "
                            + "HTTP 503 (Service: Imds, Status Code: 503, Request ID: xyz)]")
                    .build();
            assertEquals(SageMakerTransport.Classification.RETRYABLE,
                    SageMakerTransport.classify(sdke));
        }

        @Test
        @DisplayName("Walks the cause chain — IOException wrapped in RuntimeException is retryable")
        void walksCauseChain() {
            RuntimeException wrapper = new RuntimeException("oops", new IOException("netty"));
            assertEquals(SageMakerTransport.Classification.RETRYABLE, SageMakerTransport.classify(wrapper));
        }

        @Test
        @DisplayName("Unknown exception defaults to RETRYABLE (budget is the safety net)")
        void unknownDefaultsToRetryable() {
            assertEquals(SageMakerTransport.Classification.RETRYABLE,
                    SageMakerTransport.classify(new RuntimeException("mystery")));
        }

        @Test
        @DisplayName("Netty WriteTimeoutException-style RuntimeException is retryable by default")
        void nettyWriteTimeoutIsRetryable() {
            // io.netty.handler.timeout.WriteTimeoutException extends Netty's own TimeoutException
            // (NOT java.util.concurrent.TimeoutException) which extends RuntimeException. We don't
            // want to take a direct Netty compile dep just to instanceof-check it, so the new
            // default-RETRYABLE policy covers this organically.
            class WriteTimeoutException extends RuntimeException {
                WriteTimeoutException() { super(); }
            }
            assertEquals(SageMakerTransport.Classification.RETRYABLE,
                    SageMakerTransport.classify(new WriteTimeoutException()));
        }

        @Test
        @DisplayName("AWS 400 (ValidationException) is terminal — caller-side rejection")
        void aws400ValidationIsTerminal() {
            AwsServiceException ase = AwsServiceException.builder()
                    .message("invalid input")
                    .statusCode(400)
                    .awsErrorDetails(AwsErrorDetails.builder()
                            .errorCode("ValidationException")
                            .build())
                    .build();
            assertEquals(SageMakerTransport.Classification.TERMINAL, SageMakerTransport.classify(ase));
        }

        @Test
        @DisplayName("AWS 404 (ResourceNotFound) is terminal — won't appear on retry")
        void aws404IsTerminal() {
            AwsServiceException ase = AwsServiceException.builder()
                    .message("endpoint not found")
                    .statusCode(404)
                    .build();
            assertEquals(SageMakerTransport.Classification.TERMINAL, SageMakerTransport.classify(ase));
        }

        @Test
        @DisplayName("AWS 424 (Failed Dependency, SageMaker ModelError) is retryable — upstream container transient")
        void aws424IsRetryable() {
            // Mirrors the actual SageMaker burst-load error:
            //   ModelErrorException: Received server error (424) from primary with message
            //   "Failed to establish WebSocket connection"
            AwsServiceException ase = AwsServiceException.builder()
                    .message("Received server error (424) from primary with message "
                          + "\"Failed to establish WebSocket connection\"")
                    .statusCode(424)
                    .build();
            assertEquals(SageMakerTransport.Classification.RETRYABLE, SageMakerTransport.classify(ase));
        }
    }

    @Nested
    @DisplayName("StreamPublisher")
    class StreamPublisherTests {
        @Test
        @DisplayName("awaitSubscription returns false on timeout")
        void awaitSubscriptionTimeout() throws InterruptedException {
            ConcurrentLinkedQueue<RequestStreamEvent> q = new ConcurrentLinkedQueue<>();
            StreamPublisher pub = new StreamPublisher(q);
            assertFalse(pub.awaitSubscription(50, TimeUnit.MILLISECONDS),
                    "no subscriber within timeout — must report false so callers can fail fast");
        }

        @Test
        @DisplayName("awaitSubscription returns true once a subscriber arrives")
        void awaitSubscriptionSuccess() throws InterruptedException {
            ConcurrentLinkedQueue<RequestStreamEvent> q = new ConcurrentLinkedQueue<>();
            StreamPublisher pub = new StreamPublisher(q);
            pub.subscribe(noopSubscriber());
            assertTrue(pub.awaitSubscription(100, TimeUnit.MILLISECONDS));
        }

        @Test
        @DisplayName("Pending queue is shared across publisher instances — surviving an internal reset")
        void pendingPersistsAcrossPublishers() {
            ConcurrentLinkedQueue<RequestStreamEvent> shared = new ConcurrentLinkedQueue<>();

            // First publisher: send 3 events before any subscriber arrives, then complete (simulating reset).
            StreamPublisher first = new StreamPublisher(shared);
            first.send(payloadEvent("a"));
            first.send(payloadEvent("b"));
            first.send(payloadEvent("c"));
            first.complete();

            assertEquals(3, shared.size(), "events must remain in the shared queue across publishers");

            // Second publisher: subscribes and drains the queued events.
            StreamPublisher second = new StreamPublisher(shared);
            CapturingSubscriber sub = new CapturingSubscriber();
            second.subscribe(sub);

            assertEquals(3, sub.received.size(), "events queued before reset must arrive on the new stream");
            assertTrue(shared.isEmpty(), "queue must be drained after subscription");
        }

        @Test
        @DisplayName("send() with subscriber present forwards immediately, no queueing")
        void sendForwardsWhenSubscribed() {
            ConcurrentLinkedQueue<RequestStreamEvent> shared = new ConcurrentLinkedQueue<>();
            StreamPublisher pub = new StreamPublisher(shared);
            CapturingSubscriber sub = new CapturingSubscriber();
            pub.subscribe(sub);

            pub.send(payloadEvent("hello"));

            assertEquals(1, sub.received.size());
            assertTrue(shared.isEmpty());
        }
    }

    @Nested
    @DisplayName("Replay buffer")
    class ReplayBufferTests {
        private SageMakerTransport newTransport(long maxReplayBufferBytes) {
            SageMakerConfig cfg = SageMakerConfig.builder()
                    .endpointName("test")
                    .region("us-east-1")
                    .maxReplayBufferBytes(maxReplayBufferBytes)
                    .build();
            // null AWS client is fine — these tests only exercise the in-memory buffer helpers,
            // never attempt a real bidi stream.
            return new SageMakerTransport(null, cfg, "v1/listen", "");
        }

        @Test
        @DisplayName("buffer accumulates events with running byte count")
        void bufferAccumulates() {
            SageMakerTransport t = newTransport(1024);
            t.bufferForReplayForTest(payloadEvent("aaa"), 3);
            t.bufferForReplayForTest(payloadEvent("bbbbb"), 5);
            assertEquals(2, t.replayBufferSize());
            assertEquals(8L, t.replayBufferBytes());
        }

        @Test
        @DisplayName("clearReplayBuffer drops everything (the AWS-acked path)")
        void clearDropsAll() {
            SageMakerTransport t = newTransport(1024);
            t.bufferForReplayForTest(payloadEvent("a"), 1);
            t.bufferForReplayForTest(payloadEvent("b"), 1);
            t.clearReplayBufferForTest();
            assertEquals(0, t.replayBufferSize());
            assertEquals(0L, t.replayBufferBytes());
        }

        @Test
        @DisplayName("FIFO eviction once cap exceeded — newest events kept, oldest dropped")
        void evictionFifo() {
            SageMakerTransport t = newTransport(10);  // cap = 10 bytes
            RequestStreamEvent a = payloadEvent("aaaa");
            RequestStreamEvent b = payloadEvent("bbbb");
            RequestStreamEvent c = payloadEvent("cccc");
            RequestStreamEvent d = payloadEvent("dddd");
            t.bufferForReplayForTest(a, 4);   // total 4
            t.bufferForReplayForTest(b, 4);   // total 8
            t.bufferForReplayForTest(c, 4);   // total 12 → evict a, total 8
            t.bufferForReplayForTest(d, 4);   // total 12 → evict b, total 8

            // Latest two events should survive, oldest two evicted, ordering preserved.
            java.util.List<RequestStreamEvent> remaining = t.drainReplayBufferForTest();
            assertEquals(2, remaining.size());
            assertSame(c, remaining.get(0), "oldest surviving event should be 'cccc'");
            assertSame(d, remaining.get(1), "newest event should be 'dddd'");
            assertEquals(8L, t.replayBufferBytes());
        }

        @Test
        @DisplayName("maxReplayBufferBytes=0 disables buffering entirely")
        void disabledByZeroCap() {
            SageMakerTransport t = newTransport(0L);
            t.bufferForReplayForTest(payloadEvent("a"), 1);
            t.bufferForReplayForTest(payloadEvent("b"), 1);
            assertEquals(0, t.replayBufferSize(), "events must not accumulate when cap=0");
            assertEquals(0L, t.replayBufferBytes());
        }

        @Test
        @DisplayName("oversized single event is dropped immediately by the eviction loop")
        void oversizedEventCantStick() {
            SageMakerTransport t = newTransport(10);
            // A single 16-byte event exceeds the 10-byte cap; eviction loop should remove it.
            t.bufferForReplayForTest(payloadEvent("0123456789ABCDEF"), 16);
            assertEquals(0, t.replayBufferSize());
            assertEquals(0L, t.replayBufferBytes());
        }
    }

    @Nested
    @DisplayName("computeBackoff(initial, max, multiplier, attempt) — full jitter")
    class ComputeBackoffTests {
        @Test
        @DisplayName("ceiling grows exponentially up to max; range is [initial, ceiling] inclusive")
        void exponentialCeilingGrowth() {
            // initial=100, multiplier=2 → ceilings 100, 200, 400, 800, 1600, capped at 1000.
            // attempt=0: ceiling=initial=100 → degenerate range, returns 100 without RNG.
            assertEquals(100L, SageMakerTransport.computeBackoff(100, 1000, 2.0, 0, MID_RNG));
            // attempt=1: ceiling=200. Range=[100,201). MID_RNG returns origin + (bound-origin)/2 = 100 + 50 = 150.
            assertEquals(150L, SageMakerTransport.computeBackoff(100, 1000, 2.0, 1, MID_RNG));
            // attempt=4: scaled=1600, capped to ceiling=1000. Range=[100,1001). MID_RNG returns 100 + 450 = 550.
            assertEquals(550L, SageMakerTransport.computeBackoff(100, 1000, 2.0, 4, MID_RNG));
        }

        @Test
        @DisplayName("MIN_RNG returns the initial floor, MAX_RNG returns the ceiling")
        void rngBoundsRespected() {
            // attempt=2 with initial=100, mult=2: scaled=400, ceiling=400. Range=[100,401).
            assertEquals(100L, SageMakerTransport.computeBackoff(100, 1000, 2.0, 2, MIN_RNG));
            assertEquals(400L, SageMakerTransport.computeBackoff(100, 1000, 2.0, 2, MAX_RNG));
        }

        @Test
        @DisplayName("ceiling caps at max regardless of attempt")
        void ceilingCappedAtMax() {
            // High attempt count would overflow without the cap.
            assertEquals(5000L, SageMakerTransport.computeBackoff(100, 5000, 2.0, 100, MAX_RNG));
            // Even infinity scaling caps cleanly.
            assertEquals(5000L, SageMakerTransport.computeBackoff(100, 5000, 2.0, 10_000, MAX_RNG));
        }

        @Test
        @DisplayName("when ceiling == initial (attempt=0 or multiplier degenerate), returns ceiling without invoking RNG")
        void degenerateRangeReturnsCeiling() {
            java.util.concurrent.atomic.AtomicInteger rngCalls = new java.util.concurrent.atomic.AtomicInteger(0);
            java.util.function.LongBinaryOperator countingRng = (o, b) -> { rngCalls.incrementAndGet(); return o; };
            assertEquals(100L, SageMakerTransport.computeBackoff(100, 1000, 2.0, 0, countingRng));
            // multiplier=1.0 means scaled never grows beyond initial.
            assertEquals(100L, SageMakerTransport.computeBackoff(100, 1000, 1.0, 5, countingRng));
            assertEquals(0, rngCalls.get(), "RNG must not be invoked when range collapses to a single value");
        }

        @Test
        @DisplayName("with real ThreadLocalRandom: 1000 samples spread continuously across [initial, ceiling]")
        void productionRngSpreadsRetries() {
            // The whole point of this fix: in production, N concurrent retries should NOT cluster
            // at the same ceiling value. Sample many times and assert the spread is meaningful.
            int trials = 1000;
            long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
            long sum = 0;
            for (int i = 0; i < trials; i++) {
                long b = SageMakerTransport.computeBackoff(
                        100, 1000, 2.0, /*attempt*/ 4,
                        java.util.concurrent.ThreadLocalRandom.current()::nextLong);
                min = Math.min(min, b);
                max = Math.max(max, b);
                sum += b;
            }
            // attempt=4 → ceiling=1000, range=[100,1000]. Expected spread is large.
            assertTrue(min < 200, "min sample should land near initial floor; got " + min);
            assertTrue(max > 900, "max sample should land near ceiling; got " + max);
            long mean = sum / trials;
            assertTrue(mean > 400 && mean < 700,
                    "mean of uniform [100,1000] should be near 550; got " + mean);
        }
    }
}
