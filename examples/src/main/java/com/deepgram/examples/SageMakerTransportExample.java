package com.deepgram.examples;

import com.deepgram.DeepgramClient;
import com.deepgram.resources.listen.v1.types.ListenV1CloseStream;
import com.deepgram.resources.listen.v1.types.ListenV1CloseStreamType;
import com.deepgram.resources.listen.v1.types.ListenV1ResultsChannelAlternativesItem;
import com.deepgram.resources.listen.v1.websocket.V1ConnectOptions;
import com.deepgram.resources.listen.v1.websocket.V1WebSocketClient;
import com.deepgram.sagemaker.SageMakerConfig;
import com.deepgram.sagemaker.SageMakerTransportFactory;
import com.deepgram.types.ListenV1Model;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import okio.ByteString;

/**
 * Example: Streaming transcription routed through a SageMaker endpoint.
 *
 * <p>Audio is paced to real-time to match how a live microphone stream would
 * behave. The Deepgram model runs inside a SageMaker endpoint and audio goes
 * via HTTP/2 (AWS SDK), not directly to api.deepgram.com.
 *
 * <p>Usage:
 * <pre>
 *   # AWS credentials via env, profile, or IAM role
 *   export AWS_REGION=us-east-2
 *   export SAGEMAKER_ENDPOINT=my-endpoint
 *   ./gradlew :examples:run -PmainClass=com.deepgram.examples.SageMakerTransportExample
 * </pre>
 */
public class SageMakerTransportExample {

    public static void main(String[] args) throws Exception {
        String endpointName = System.getenv().getOrDefault(
                "SAGEMAKER_ENDPOINT", "deepgram-nova-3");

        String region = System.getenv().getOrDefault(
                "AWS_REGION", "us-west-2");

        String modelName = System.getenv().getOrDefault(
                "DEEPGRAM_MODEL", "nova-3");
        ListenV1Model model = ListenV1Model.valueOf(modelName);

        // Create transport factory
        SageMakerConfig config = SageMakerConfig.builder()
                .endpointName(endpointName)
                .region(region)
                .build();

        SageMakerTransportFactory factory = new SageMakerTransportFactory(config);

        // Build the SDK client with the SageMaker transport
        // The apiKey is unused — SageMaker uses AWS credentials
        DeepgramClient client = DeepgramClient.builder()
                .apiKey("unused")
                .transportFactory(factory)
                .build();

        System.out.println("Connecting via SageMaker endpoint: " + endpointName);
        System.out.println("Model: " + modelName);

        // From here, the code is identical to any other Deepgram SDK usage
        V1WebSocketClient wsClient = client.listen().v1().v1WebSocket();
        CountDownLatch done = new CountDownLatch(1);

        wsClient.onResults(result -> {
            if (result.getChannel() != null
                    && result.getChannel().getAlternatives() != null
                    && !result.getChannel().getAlternatives().isEmpty()) {
                ListenV1ResultsChannelAlternativesItem alt =
                        result.getChannel().getAlternatives().get(0);
                String transcript = alt.getTranscript();
                if (transcript != null && !transcript.isEmpty()) {
                    boolean isFinal = result.getIsFinal().orElse(false);
                    System.out.printf("%s %s%n",
                            isFinal ? "[final]  " : "[interim]",
                            transcript);
                }
            }
        });

        wsClient.onError(error -> {
            System.err.println("Error: " + error.getMessage());
            done.countDown();
        });

        wsClient.onDisconnected(reason -> {
            System.out.println("Stream complete (code " + reason.getCode() + ")");
            done.countDown();
        });

        // Connect
        CompletableFuture<Void> connectFuture = wsClient.connect(
                V1ConnectOptions.builder()
                        .model(model)
                        .build());
        connectFuture.get(10, TimeUnit.SECONDS);

        // Send audio file paced to real-time
        Path audioFile = Path.of("spacewalk.wav");
        if (java.nio.file.Files.exists(audioFile)) {
            // Parse WAV header for pacing info
            int sampleRate;
            int blockAlign;
            try (RandomAccessFile raf = new RandomAccessFile(audioFile.toFile(), "r")) {
                raf.skipBytes(24);
                byte[] srBytes = new byte[4];
                raf.read(srBytes);
                sampleRate = ByteBuffer.wrap(srBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();

                raf.skipBytes(4); // skip byte rate
                byte[] baBytes = new byte[2];
                raf.read(baBytes);
                blockAlign = ByteBuffer.wrap(baBytes).order(ByteOrder.LITTLE_ENDIAN).getShort() & 0xFFFF;
            }

            // Read entire file (including WAV header — the model needs it)
            byte[] audio = java.nio.file.Files.readAllBytes(audioFile);
            int chunkSize = 8192;
            double framesPerChunk = (double) chunkSize / blockAlign;
            long sleepMicros = (long) (framesPerChunk / sampleRate * 1_000_000);

            System.out.printf("Streaming WAV: %d Hz, block align %d, pacing %d µs per chunk%n",
                    sampleRate, blockAlign, sleepMicros);

            for (int i = 0; i < audio.length; i += chunkSize) {
                int end = Math.min(i + chunkSize, audio.length);
                byte[] chunk = new byte[end - i];
                System.arraycopy(audio, i, chunk, 0, chunk.length);
                wsClient.sendMedia(ByteString.of(chunk));
                TimeUnit.MICROSECONDS.sleep(sleepMicros);
            }

            // Signal end of audio
            wsClient.sendCloseStream(
                    ListenV1CloseStream.builder()
                            .type(ListenV1CloseStreamType.CLOSE_STREAM)
                            .build());
        } else {
            System.err.println("Place spacewalk.wav in the project root to test.");
            System.err.println("Download from: https://dpgr.am/spacewalk.wav");
        }

        done.await(60, TimeUnit.SECONDS);
        wsClient.disconnect();
        factory.shutdown();
        System.out.println("Done.");
    }
}
