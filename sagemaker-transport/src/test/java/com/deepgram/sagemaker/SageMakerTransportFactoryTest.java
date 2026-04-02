package com.deepgram.sagemaker;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.deepgram.core.transport.DeepgramTransport;
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
}
