package com.deepgram.sagemaker;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.deepgram.core.transport.DeepgramTransport;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import software.amazon.awssdk.services.sagemakerruntimehttp2.SageMakerRuntimeHttp2AsyncClient;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SageMakerTransport}.
 *
 * <p>Validates initial state, close idempotency, and basic behavior
 * without requiring AWS credentials.
 */
class SageMakerTransportTest {

  private SageMakerConfig config;
  private SageMakerTransportFactory factory;

  @BeforeEach
  void setUp() {
    config = SageMakerConfig.builder().endpointName("test-endpoint").build();
    // Use a mock client so no real AWS connection is made
    SageMakerRuntimeHttp2AsyncClient mockClient = mock(SageMakerRuntimeHttp2AsyncClient.class);
    factory = new SageMakerTransportFactory(config, mockClient);
  }

  @Test
  void initialStateIsOpen() {
    DeepgramTransport transport =
        factory.create("wss://api.deepgram.com/v1/listen", Map.of());

    assertTrue(transport.isOpen());
  }

  @Test
  void closeIsIdempotent() {
    DeepgramTransport transport =
        factory.create("wss://api.deepgram.com/v1/listen", Map.of());

    transport.close();
    assertFalse(transport.isOpen());

    // Second close should not throw
    assertDoesNotThrow(transport::close);
    assertFalse(transport.isOpen());
  }

  @Test
  void sendFailsAfterClose() {
    DeepgramTransport transport =
        factory.create("wss://api.deepgram.com/v1/listen", Map.of());

    transport.close();

    // Send after close should complete exceptionally
    var future = transport.sendBinary(new byte[] {1, 2, 3});
    assertTrue(future.isCompletedExceptionally());
  }

  @Test
  void sendTextFailsAfterClose() {
    DeepgramTransport transport =
        factory.create("wss://api.deepgram.com/v1/listen", Map.of());

    transport.close();

    var future = transport.sendText("{\"type\":\"KeepAlive\"}");
    assertTrue(future.isCompletedExceptionally());
  }

  @Test
  void canRegisterListeners() {
    DeepgramTransport transport =
        factory.create("wss://api.deepgram.com/v1/listen", Map.of());

    AtomicBoolean called = new AtomicBoolean(false);

    // Should not throw — just registers callbacks
    assertDoesNotThrow(() -> transport.onTextMessage(msg -> called.set(true)));
    assertDoesNotThrow(() -> transport.onBinaryMessage(bytes -> {}));
    assertDoesNotThrow(() -> transport.onError(err -> {}));
    assertDoesNotThrow(() -> transport.onClose((code, reason) -> {}));
  }

  @Test
  void onOpenFiresImmediately() {
    DeepgramTransport transport =
        factory.create("wss://api.deepgram.com/v1/listen", Map.of());

    AtomicBoolean opened = new AtomicBoolean(false);
    transport.onOpen(() -> opened.set(true));
    assertTrue(opened.get());
  }
}
