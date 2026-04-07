# Changelog

## 0.1.0 (2026-04-07)


### Features

* add maven central publishing and transport fixes ([2113022](https://github.com/deepgram/deepgram-java-sdk-transport-sagemaker/commit/2113022f5b015f3b5ec793cc512354687bd4a48f))
* initial release of SageMaker HTTP/2 bidirectional streaming transport ([c434257](https://github.com/deepgram/deepgram-java-sdk-transport-sagemaker/commit/c434257eaaa89567fb77f52be4f726cbb9e4d00e))
* maven central publishing and transport fixes ([7cc07fd](https://github.com/deepgram/deepgram-java-sdk-transport-sagemaker/commit/7cc07fd3d617e9914c0db07fd4fe33cffcb24c86))


### Miscellaneous Chores

* trigger initial release ([f6f1c99](https://github.com/deepgram/deepgram-java-sdk-transport-sagemaker/commit/f6f1c99749c1645c9145bbc727449e2394d3158b))
* trigger initial release ([d7aa46c](https://github.com/deepgram/deepgram-java-sdk-transport-sagemaker/commit/d7aa46c7f54b4d5ff66da7d14cac1759619dd834))

## 0.1.0 (unreleased)

### Features

* Initial deepgram-sagemaker transport package for Java
* `SageMakerTransport` implementing `DeepgramTransport` from the Deepgram Java SDK
* `SageMakerTransportFactory` implementing `DeepgramTransportFactory`
* `SageMakerConfig` builder for endpoint name, region, content type, and accept type
* HTTP/2 response streaming via AWS SDK v2 async client
* Example showing SDK integration with `DeepgramClient.builder().transportFactory()`
