package com.deepgram.sagemaker;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Validates that public API classes are accessible.
 *
 * <p>Mirrors the Python SDK's TestExports — ensures the package exports the expected types.
 */
class SageMakerExportsTest {

  @Test
  void sageMakerTransportFactoryIsPublic() {
    assertDoesNotThrow(() -> Class.forName("com.deepgram.sagemaker.SageMakerTransportFactory"));
  }

  @Test
  void sageMakerConfigIsPublic() {
    assertDoesNotThrow(() -> Class.forName("com.deepgram.sagemaker.SageMakerConfig"));
  }

  @Test
  void sageMakerTransportExists() {
    assertDoesNotThrow(() -> Class.forName("com.deepgram.sagemaker.SageMakerTransport"));
  }
}
