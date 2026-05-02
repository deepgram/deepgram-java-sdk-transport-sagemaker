package com.deepgram.sagemaker;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.deepgram.core.ReconnectingWebSocketListener;
import com.deepgram.core.transport.DeepgramTransport;
import java.time.Duration;
import java.util.Map;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sagemakerruntimehttp2.SageMakerRuntimeHttp2AsyncClient;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SageMakerTransportFactory}.
 *
 * <p>Validates factory creation, config defaults, URL parsing,
 * and transport instantiation.
 */
class SageMakerTransportFactoryTest {

  @Test
  void factoryCreatesTransportWithValidConfig() {
    SageMakerConfig config =
        SageMakerConfig.builder().endpointName("my-endpoint").region("us-west-2").build();

    SageMakerRuntimeHttp2AsyncClient mockClient = mock(SageMakerRuntimeHttp2AsyncClient.class);
    SageMakerTransportFactory factory = new SageMakerTransportFactory(config, mockClient);

    DeepgramTransport transport =
        factory.create(
            "wss://api.deepgram.com/v1/listen?model=nova-3",
            Map.of("Authorization", "Token test-key"));

    assertNotNull(transport);
  }

  @Test
  void factoryDefaultRegionIsUsWest2() {
    SageMakerConfig config = SageMakerConfig.builder().endpointName("my-endpoint").build();

    assertEquals(Region.US_WEST_2, config.region());
  }

  @Test
  void factoryRejectsNullEndpointName() {
    assertThrows(
        IllegalArgumentException.class, () -> SageMakerConfig.builder().region("us-east-1").build());
  }

  @Test
  void factoryRejectsBlankEndpointName() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SageMakerConfig.builder().endpointName("  ").build());
  }

  @Test
  void factoryAcceptsCustomRegion() {
    SageMakerConfig config =
        SageMakerConfig.builder().endpointName("my-endpoint").region("eu-west-1").build();

    assertEquals(Region.EU_WEST_1, config.region());
  }

  @Test
  void configDefaultsAreLenientForHighConcurrency() {
    SageMakerConfig config = SageMakerConfig.builder().endpointName("my-endpoint").build();

    assertEquals(Duration.ofSeconds(30), config.connectionTimeout());
    assertEquals(Duration.ofSeconds(60), config.connectionAcquireTimeout());
    assertEquals(Duration.ofSeconds(60), config.subscriptionTimeout());
    assertEquals(500, config.maxConcurrency());
  }

  @Test
  void configAcceptsCustomTimeoutsAndConcurrency() {
    SageMakerConfig config =
        SageMakerConfig.builder()
            .endpointName("my-endpoint")
            .connectionTimeout(Duration.ofSeconds(5))
            .connectionAcquireTimeout(Duration.ofSeconds(15))
            .subscriptionTimeout(Duration.ofSeconds(45))
            .maxConcurrency(1000)
            .build();

    assertEquals(Duration.ofSeconds(5), config.connectionTimeout());
    assertEquals(Duration.ofSeconds(15), config.connectionAcquireTimeout());
    assertEquals(Duration.ofSeconds(45), config.subscriptionTimeout());
    assertEquals(1000, config.maxConcurrency());
  }

  @Test
  void configRejectsNonPositiveTimeouts() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SageMakerConfig.builder().endpointName("e").connectionTimeout(Duration.ZERO).build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SageMakerConfig.builder()
                .endpointName("e")
                .connectionAcquireTimeout(Duration.ofSeconds(-1))
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SageMakerConfig.builder()
                .endpointName("e")
                .subscriptionTimeout(null)
                .build());
  }

  @Test
  void configRejectsNonPositiveMaxConcurrency() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SageMakerConfig.builder().endpointName("e").maxConcurrency(0).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> SageMakerConfig.builder().endpointName("e").maxConcurrency(-1).build());
  }

  @Test
  void retryConfigDefaults() {
    SageMakerConfig config = SageMakerConfig.builder().endpointName("my-endpoint").build();

    assertEquals(5, config.maxRetries());
    assertEquals(Duration.ofMillis(100), config.initialBackoff());
    assertEquals(Duration.ofSeconds(5), config.maxBackoff());
    assertEquals(2.0, config.backoffMultiplier());
    assertEquals(Duration.ofSeconds(30), config.retryBudget());
  }

  @Test
  void retryConfigAcceptsCustomValues() {
    SageMakerConfig config =
        SageMakerConfig.builder()
            .endpointName("my-endpoint")
            .maxRetries(10)
            .initialBackoff(Duration.ofMillis(50))
            .maxBackoff(Duration.ofSeconds(20))
            .backoffMultiplier(3.0)
            .retryBudget(Duration.ofMinutes(2))
            .build();

    assertEquals(10, config.maxRetries());
    assertEquals(Duration.ofMillis(50), config.initialBackoff());
    assertEquals(Duration.ofSeconds(20), config.maxBackoff());
    assertEquals(3.0, config.backoffMultiplier());
    assertEquals(Duration.ofMinutes(2), config.retryBudget());
  }

  @Test
  void retryConfigValidates() {
    assertThrows(IllegalArgumentException.class,
        () -> SageMakerConfig.builder().endpointName("e").maxRetries(-1).build());
    assertThrows(IllegalArgumentException.class,
        () -> SageMakerConfig.builder().endpointName("e").initialBackoff(Duration.ZERO).build());
    assertThrows(IllegalArgumentException.class,
        () -> SageMakerConfig.builder().endpointName("e").maxBackoff(Duration.ofSeconds(-1)).build());
    assertThrows(IllegalArgumentException.class,
        () -> SageMakerConfig.builder().endpointName("e").backoffMultiplier(0.5).build());
    assertThrows(IllegalArgumentException.class,
        () -> SageMakerConfig.builder().endpointName("e").retryBudget(null).build());
  }

  @Test
  void retryConfigRejectsInitialGreaterThanMax() {
    assertThrows(IllegalArgumentException.class,
        () -> SageMakerConfig.builder()
                .endpointName("e")
                .initialBackoff(Duration.ofSeconds(10))
                .maxBackoff(Duration.ofSeconds(5))
                .build());
  }

  @Test
  void factoryDeclaresMaxRetriesZeroForReconnectOptions() {
    // The plugin declares maxRetries(0) so the SDK's wrapper-level reconnect doesn't compound
    // SageMaker's internal retries into a storm.
    SageMakerConfig config = SageMakerConfig.builder().endpointName("my-endpoint").build();
    SageMakerRuntimeHttp2AsyncClient mockClient = mock(SageMakerRuntimeHttp2AsyncClient.class);
    SageMakerTransportFactory factory = new SageMakerTransportFactory(config, mockClient);

    ReconnectingWebSocketListener.ReconnectOptions opts = factory.reconnectOptions();
    assertNotNull(opts);
    assertEquals(0, opts.maxRetries);
  }

  // -------------------------------------------------------------------------
  // Shared-client pool tests. The default constructor backs the factory with a process-wide
  // shared SageMakerRuntimeHttp2AsyncClient keyed by config fingerprint, so naive code that
  // builds a fresh factory per stream still benefits from a single Netty pool underneath.
  // -------------------------------------------------------------------------

  @Test
  void defaultConstructorReusesSharedClientAcrossFactoriesWithSameConfig() {
    // Reset the shared pool to isolate this test from other tests in the class.
    SageMakerTransportFactory.shutdownAllSharedClients();
    try {
      SageMakerConfig configA = SageMakerConfig.builder()
          .endpointName("endpoint-A")  // endpoint name does NOT affect the shared client
          .region("us-east-1")
          .build();
      SageMakerConfig configB = SageMakerConfig.builder()
          .endpointName("endpoint-B")  // different endpoint, same Netty-relevant config
          .region("us-east-1")
          .build();

      SageMakerTransportFactory f1 = new SageMakerTransportFactory(configA);
      SageMakerTransportFactory f2 = new SageMakerTransportFactory(configB);

      // Use reflection sparingly — verify both factories point at the same underlying smClient.
      assertSame(getSmClient(f1), getSmClient(f2),
          "factories with same Netty-relevant config must share one smClient");
    } finally {
      SageMakerTransportFactory.shutdownAllSharedClients();
    }
  }

  @Test
  void defaultConstructorBuildsDistinctSharedClientsForDifferentConfigs() {
    SageMakerTransportFactory.shutdownAllSharedClients();
    try {
      SageMakerConfig configEast = SageMakerConfig.builder()
          .endpointName("e").region("us-east-1").build();
      SageMakerConfig configWest = SageMakerConfig.builder()
          .endpointName("e").region("us-west-2").build();
      SageMakerConfig configEastBigPool = SageMakerConfig.builder()
          .endpointName("e").region("us-east-1").maxConcurrency(1000).build();

      SageMakerTransportFactory east1 = new SageMakerTransportFactory(configEast);
      SageMakerTransportFactory east2 = new SageMakerTransportFactory(configEast);
      SageMakerTransportFactory west = new SageMakerTransportFactory(configWest);
      SageMakerTransportFactory eastBig = new SageMakerTransportFactory(configEastBigPool);

      assertSame(getSmClient(east1), getSmClient(east2));
      assertNotSame(getSmClient(east1), getSmClient(west));
      assertNotSame(getSmClient(east1), getSmClient(eastBig));
    } finally {
      SageMakerTransportFactory.shutdownAllSharedClients();
    }
  }

  @Test
  void byoClientConstructorIsNotPooled() {
    SageMakerTransportFactory.shutdownAllSharedClients();
    try {
      SageMakerConfig config = SageMakerConfig.builder().endpointName("e").build();
      SageMakerRuntimeHttp2AsyncClient mockClient = mock(SageMakerRuntimeHttp2AsyncClient.class);

      SageMakerTransportFactory byo = new SageMakerTransportFactory(config, mockClient);
      SageMakerTransportFactory shared = new SageMakerTransportFactory(config);

      assertSame(mockClient, getSmClient(byo), "BYO factory must use the provided client");
      assertNotSame(mockClient, getSmClient(shared),
          "Shared factory must build/lookup its own client, not steal the BYO mock");
    } finally {
      SageMakerTransportFactory.shutdownAllSharedClients();
    }
  }

  @Test
  void shutdownIsNoopForBoth() {
    // factory.shutdown() must not close shared or BYO clients — lifecycle belongs elsewhere.
    SageMakerTransportFactory.shutdownAllSharedClients();
    try {
      SageMakerConfig config = SageMakerConfig.builder().endpointName("e").build();
      SageMakerRuntimeHttp2AsyncClient mockClient = mock(SageMakerRuntimeHttp2AsyncClient.class);

      new SageMakerTransportFactory(config, mockClient).shutdown();
      verify(mockClient, never()).close();

      SageMakerTransportFactory shared = new SageMakerTransportFactory(config);
      shared.shutdown();
      // We can't easily verify .close() wasn't called on a real client without re-fetching;
      // the assertion above on mockClient is the strong guarantee. The shared variant is
      // documented as no-op via Javadoc.
    } finally {
      SageMakerTransportFactory.shutdownAllSharedClients();
    }
  }

  @Test
  void shutdownAllSharedClientsClearsThePool() {
    SageMakerTransportFactory.shutdownAllSharedClients();
    SageMakerConfig config = SageMakerConfig.builder().endpointName("e").build();

    SageMakerTransportFactory before = new SageMakerTransportFactory(config);
    SageMakerRuntimeHttp2AsyncClient firstClient = getSmClient(before);

    SageMakerTransportFactory.shutdownAllSharedClients();

    SageMakerTransportFactory after = new SageMakerTransportFactory(config);
    SageMakerRuntimeHttp2AsyncClient secondClient = getSmClient(after);

    assertNotSame(firstClient, secondClient,
        "shutdownAllSharedClients must clear the pool so the next factory builds a fresh client");

    SageMakerTransportFactory.shutdownAllSharedClients();
  }

  /** Reflection helper — read the package-private smClient field set by the constructor. */
  private static SageMakerRuntimeHttp2AsyncClient getSmClient(SageMakerTransportFactory f) {
    try {
      java.lang.reflect.Field fld = SageMakerTransportFactory.class.getDeclaredField("smClient");
      fld.setAccessible(true);
      return (SageMakerRuntimeHttp2AsyncClient) fld.get(f);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
