# Changelog

## 0.1.0 (unreleased)

### Features

* Initial deepgram-sagemaker transport package for Java
* `SageMakerTransport` implementing `DeepgramTransport` from the Deepgram Java SDK
* `SageMakerTransportFactory` implementing `DeepgramTransportFactory`
* `SageMakerConfig` builder for endpoint name, region, content type, and accept type
* HTTP/2 response streaming via AWS SDK v2 async client
* Example showing SDK integration with `DeepgramClient.builder().transportFactory()`
