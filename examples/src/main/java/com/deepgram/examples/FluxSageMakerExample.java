package com.deepgram.examples;

import com.deepgram.DeepgramClient;
import com.deepgram.resources.listen.v2.types.ListenV2CloseStream;
import com.deepgram.resources.listen.v2.types.ListenV2CloseStreamType;
import com.deepgram.resources.listen.v2.types.ListenV2TurnInfoEvent;
import com.deepgram.resources.listen.v2.websocket.V2ConnectOptions;
import com.deepgram.resources.listen.v2.websocket.V2WebSocketClient;
import com.deepgram.sagemaker.SageMakerConfig;
import com.deepgram.sagemaker.SageMakerTransportFactory;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import okio.ByteString;

/**
 * Flux model transcription via SageMaker endpoint using Listen V2.
 *
 * <p>Flux is a V2 model and uses the {@code v2/listen} endpoint path with
 * turn-based transcription. Audio is paced to real-time.
 *
 * <p>Usage:
 * <pre>
 *   export SAGEMAKER_ENDPOINT=deepgram-flux-greg
 *   export AWS_REGION=us-east-2
 *   ./gradlew :examples:run -PmainClass=com.deepgram.examples.FluxSageMakerExample
 * </pre>
 */
public class FluxSageMakerExample {

    public static void main(String[] args) throws Exception {
        String endpointName = System.getenv().getOrDefault("SAGEMAKER_ENDPOINT", "deepgram-flux");
        String region = System.getenv().getOrDefault("AWS_REGION", "us-west-2");

        Path audioFile = args.length > 0 ? Path.of(args[0]) : Path.of("spacewalk.wav");
        if (!Files.exists(audioFile)) {
            System.err.println("Audio file not found: " + audioFile);
            System.err.println("Download from: https://dpgr.am/spacewalk.wav");
            System.exit(1);
        }

        SageMakerConfig config = SageMakerConfig.builder()
                .endpointName(endpointName)
                .region(region)
                .build();
        SageMakerTransportFactory factory = new SageMakerTransportFactory(config);

        DeepgramClient client = DeepgramClient.builder()
                .apiKey("unused")
                .transportFactory(factory)
                .build();

        System.out.println("Flux transcription via SageMaker (Listen V2)");
        System.out.println("Endpoint: " + endpointName);
        System.out.println("Region:   " + region);
        System.out.println();

        // V2 WebSocket client for Flux
        V2WebSocketClient wsClient = client.listen().v2().v2WebSocket();
        CountDownLatch done = new CountDownLatch(1);

        wsClient.onTurnInfo(turnInfo -> {
            String transcript = turnInfo.getTranscript();
            ListenV2TurnInfoEvent event = turnInfo.getEvent();
            double turnIndex = turnInfo.getTurnIndex();

            if (transcript != null && !transcript.isEmpty()) {
                System.out.printf("[%s] turn=%.0f  %s%n", event, turnIndex, transcript);
            }
        });

        wsClient.onError(error -> {
            System.err.println("Error: " + error.getMessage());
            done.countDown();
        });

        wsClient.onDisconnected(reason -> {
            System.out.println("Closed (code: " + reason.getCode() + ")");
            done.countDown();
        });

        // Connect — V2 uses model name as string via additionalProperty
        CompletableFuture<Void> connectFuture = wsClient.connect(
                V2ConnectOptions.builder()
                        .model("flux-general-en")
                        .build());
        connectFuture.get(30, TimeUnit.SECONDS);
        System.out.println("Connected. Streaming audio...\n");

        // Parse WAV header for pacing
        int sampleRate;
        int blockAlign;
        try (RandomAccessFile raf = new RandomAccessFile(audioFile.toFile(), "r")) {
            raf.skipBytes(24);
            byte[] srBytes = new byte[4];
            raf.read(srBytes);
            sampleRate = ByteBuffer.wrap(srBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();

            raf.skipBytes(4);
            byte[] baBytes = new byte[2];
            raf.read(baBytes);
            blockAlign = ByteBuffer.wrap(baBytes).order(ByteOrder.LITTLE_ENDIAN).getShort() & 0xFFFF;
        }

        // Read and send audio paced to real-time
        byte[] audio = Files.readAllBytes(audioFile);
        int chunkSize = 8192;
        double framesPerChunk = (double) chunkSize / blockAlign;
        long sleepMicros = (long) (framesPerChunk / sampleRate * 1_000_000);

        System.out.printf("Streaming WAV: %d Hz, block align %d%n%n", sampleRate, blockAlign);

        for (int i = 0; i < audio.length; i += chunkSize) {
            int end = Math.min(i + chunkSize, audio.length);
            byte[] chunk = new byte[end - i];
            System.arraycopy(audio, i, chunk, 0, chunk.length);
            wsClient.sendMedia(ByteString.of(chunk));
            TimeUnit.MICROSECONDS.sleep(sleepMicros);
        }

        wsClient.sendCloseStream(
                ListenV2CloseStream.builder()
                        .type(ListenV2CloseStreamType.CLOSE_STREAM)
                        .build());

        done.await(60, TimeUnit.SECONDS);
        wsClient.disconnect();
        factory.shutdown();
        System.out.println("Done.");
    }
}
