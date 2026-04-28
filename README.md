# Deepgram SageMaker Transport for Java

[![Java Version](https://img.shields.io/badge/java-%3E%3D11-blue)](https://adoptium.net/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

SageMaker transport for the [Deepgram Java SDK](https://github.com/deepgram/deepgram-java-sdk). Uses AWS SageMaker's HTTP/2 bidirectional streaming API as an alternative to WebSocket, allowing transparent switching between Deepgram Cloud and Deepgram on SageMaker.

## Installation

### Gradle

```groovy
dependencies {
    implementation 'com.deepgram:deepgram-java-sdk:0.2.1'
    implementation 'com.deepgram:deepgram-sagemaker:0.1.2' // x-release-please-version
}
```

### Maven

```xml
<dependency>
    <groupId>com.deepgram</groupId>
    <artifactId>deepgram-sagemaker</artifactId>
    <version>0.1.2</version> <!-- x-release-please-version -->
</dependency>
```

## Requirements

- Java 11+
- [Deepgram Java SDK](https://github.com/deepgram/deepgram-java-sdk) v0.2.1+
- AWS credentials configured (environment variables, shared credentials file, or IAM role)
- A Deepgram model deployed to an AWS SageMaker endpoint

## Authentication

This transport uses **AWS credentials**, not Deepgram API keys. Authentication is handled by the AWS SDK's default credential provider chain:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. Shared credentials file (`~/.aws/credentials`)
3. IAM instance role (when running on EC2/ECS/Lambda)

The `apiKey` parameter on the Deepgram client builder is unused when a SageMaker transport is configured, but a value must be provided (the builder requires it).

## Quickstart

```java
import com.deepgram.DeepgramClient;
import com.deepgram.sagemaker.SageMakerConfig;
import com.deepgram.sagemaker.SageMakerTransportFactory;
import com.deepgram.resources.listen.v1.websocket.V1ConnectOptions;
import com.deepgram.resources.listen.v1.websocket.V1WebSocketClient;
import com.deepgram.types.ListenV1Model;

// 1. Configure the SageMaker transport
SageMakerTransportFactory factory = new SageMakerTransportFactory(
    SageMakerConfig.builder()
        .endpointName("my-deepgram-endpoint")
        .region("us-west-2")
        .build()
);

// 2. Create the Deepgram client with the transport factory
DeepgramClient client = DeepgramClient.builder()
    .apiKey("unused")
    .transportFactory(factory)
    .build();

// 3. Use the SDK exactly as normal
V1WebSocketClient ws = client.listen().v1().v1WebSocket();

ws.onResults(results -> {
    String transcript = results.getChannel()
        .getAlternatives().get(0)
        .getTranscript();
    System.out.println(transcript);
});

ws.connect(V1ConnectOptions.builder()
    .model(ListenV1Model.NOVA3)
    .build());

ws.sendMedia(audioBytes);
ws.close();
```

The transport is transparent — the SDK API is identical whether using Deepgram Cloud or Deepgram on SageMaker.

## Configuration

### SageMakerConfig

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `endpointName` | Yes | — | SageMaker endpoint name |
| `region` | No | `us-west-2` | AWS region |
| `connectionTimeout` | No | `30s` | Max time for the underlying TCP/TLS connect (AWS Netty default is 2&nbsp;s — bumped here so cold-start endpoints under burst load have time to accept TLS handshakes). |
| `connectionAcquireTimeout` | No | `60s` | Max time to acquire a connection from the Netty pool (AWS Netty default is 10&nbsp;s — bumped so a 200&ndash;500-stream burst doesn't drain the acquire pool). |
| `subscriptionTimeout` | No | `60s` | Max time the transport waits for the AWS SDK to subscribe to the bidi-stream input publisher before failing the first send. |
| `maxConcurrency` | No | `500` | Max simultaneous in-flight HTTP/2 streams across the shared Netty pool. With `maxStreams=1` this is the cap on simultaneous bidirectional streams. |

```java
SageMakerConfig config = SageMakerConfig.builder()
    .endpointName("my-deepgram-endpoint")
    .region("us-east-2")
    .build();
```

#### High-concurrency notes

The transport's defaults are tuned for high-burst workloads (large numbers of
streams opened in a tight loop against an endpoint that may need to scale up).
If you're opening 200&ndash;500 streams simultaneously against a cold endpoint,
the AWS Netty defaults (2&nbsp;s connect / 10&nbsp;s acquire) will fire before
the load balancer has accepted all of the inbound TLS handshakes &mdash; you'll
see a wave of `connection acquire` and `connect timed out` errors that look
like server-side problems but are really client-side fail-fast tripping early.

This transport ships with more lenient defaults (30&nbsp;s / 60&nbsp;s) so the
common high-concurrency path works out of the box. Tighten them if you need
fail-fast behavior in low-latency pipelines:

```java
SageMakerConfig config = SageMakerConfig.builder()
    .endpointName("my-deepgram-endpoint")
    .region("us-east-2")
    .connectionTimeout(Duration.ofSeconds(5))
    .connectionAcquireTimeout(Duration.ofSeconds(15))
    .build();
```

### Custom AWS Client

For custom credential providers, proxy configuration, or testing:

```java
import software.amazon.awssdk.services.sagemakerruntimehttp2.SageMakerRuntimeHttp2AsyncClient;

SageMakerRuntimeHttp2AsyncClient customClient = SageMakerRuntimeHttp2AsyncClient.builder()
    .region(Region.US_WEST_2)
    .credentialsProvider(myCredentialsProvider)
    .build();

SageMakerTransportFactory factory = new SageMakerTransportFactory(config, customClient);
```

## How It Works

The standard Deepgram SDK connects via WebSocket to `wss://api.deepgram.com`. This transport replaces that connection with HTTP/2 streaming to your SageMaker endpoint:

```
Standard:   SDK → WebSocket → api.deepgram.com → Deepgram cloud
SageMaker:  SDK → HTTP/2    → SageMaker endpoint → Your Deepgram model
```

Under the hood, the transport uses the AWS SDK v2's `InvokeEndpointWithBidirectionalStream` API for true bidirectional HTTP/2 streaming — audio chunks are sent and transcript responses received concurrently over a single connection.

## Project Structure

This is a multi-module Gradle project:

```
deepgram-sagemaker-java/
├── sagemaker-transport/    # SageMaker implementation (SageMakerTransport, SageMakerConfig)
└── examples/               # Usage examples
```

## Examples

### File Transcription (STT)

```bash
# AWS credentials must be configured
export AWS_REGION=us-east-2
export SAGEMAKER_ENDPOINT=my-deepgram-endpoint
./gradlew :examples:run -PmainClass=com.deepgram.examples.SageMakerTransportExample
```

### Live Microphone (STT — Nova-3)

```bash
export AWS_REGION=us-east-2
export SAGEMAKER_ENDPOINT=my-deepgram-endpoint
./gradlew :examples:run -PmainClass=com.deepgram.examples.LiveMicSageMakerExample
```

### Live Microphone (STT — Flux V2)

```bash
export AWS_REGION=us-east-2
export SAGEMAKER_ENDPOINT=my-deepgram-flux-endpoint
./gradlew :examples:run -PmainClass=com.deepgram.examples.LiveMicFluxSageMakerExample
```

## Dependencies

The SageMaker transport adds these dependencies (not included in the core SDK):

| Dependency | Version | Purpose |
|-----------|---------|---------|
| `software.amazon.awssdk:sagemakerruntimehttp2` | 2.42.x | SageMaker HTTP/2 bidirectional streaming client |
| `software.amazon.awssdk:netty-nio-client` | 2.42.x | HTTP/2 via Netty |

## Development

Requires Java 11+. If Gradle can't find your JDK, set it in `gradle.properties`:

```properties
org.gradle.java.home=/path/to/your/jdk
```

Run tests:

```bash
./gradlew build
```

## Getting Help

- [Deepgram Documentation](https://developers.deepgram.com)
- [Deepgram Java SDK](https://github.com/deepgram/deepgram-java-sdk)
- [AWS SageMaker Documentation](https://docs.aws.amazon.com/sagemaker/)
- [Report Issues](https://github.com/deepgram/deepgram-java-sdk-transport-sagemaker/issues)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
