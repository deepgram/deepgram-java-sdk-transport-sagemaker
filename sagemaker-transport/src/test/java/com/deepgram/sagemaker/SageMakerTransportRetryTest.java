package com.deepgram.sagemaker;

import static org.junit.jupiter.api.Assertions.*;

import com.deepgram.sagemaker.SageMakerTransport.StreamPublisher;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
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
        @DisplayName("Walks the cause chain — IOException wrapped in RuntimeException is retryable")
        void walksCauseChain() {
            RuntimeException wrapper = new RuntimeException("oops", new IOException("netty"));
            assertEquals(SageMakerTransport.Classification.RETRYABLE, SageMakerTransport.classify(wrapper));
        }

        @Test
        @DisplayName("Unknown exception defaults to terminal")
        void unknownDefaultsToTerminal() {
            assertEquals(SageMakerTransport.Classification.TERMINAL,
                    SageMakerTransport.classify(new RuntimeException("mystery")));
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
}
