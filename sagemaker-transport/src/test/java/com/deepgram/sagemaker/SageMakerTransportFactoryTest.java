package com.deepgram.sagemaker;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

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
}
