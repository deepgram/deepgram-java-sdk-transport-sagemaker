package com.deepgram.examples;

import com.deepgram.DeepgramClient;
import com.deepgram.resources.listen.v1.types.ListenV1CloseStream;
import com.deepgram.resources.listen.v1.types.ListenV1CloseStreamType;
import com.deepgram.resources.listen.v1.types.ListenV1ResultsChannelAlternativesItem;
import com.deepgram.resources.listen.v1.websocket.V1ConnectOptions;
import com.deepgram.resources.listen.v1.websocket.V1WebSocketClient;
import com.deepgram.sagemaker.SageMakerConfig;
import com.deepgram.sagemaker.SageMakerTransportFactory;
import com.deepgram.types.ListenV1Encoding;
import com.deepgram.types.ListenV1InterimResults;
import com.deepgram.types.ListenV1Model;
import com.deepgram.types.ListenV1SampleRate;

import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.DataLine;
import javax.sound.sampled.TargetDataLine;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import okio.ByteString;

/**
 * Live microphone transcription via a SageMaker endpoint.
 *
 * <p>Captures audio from the system microphone and streams it to a Deepgram
 * model running on SageMaker for real-time transcription. Press Ctrl+C to stop.
 *
 * <p>Usage:
 * <pre>
 *   export SAGEMAKER_ENDPOINT=my-deepgram-endpoint
 *   export AWS_REGION=us-east-2
 *   ./gradlew :examples:run -PmainClass=com.deepgram.examples.LiveMicSageMakerExample
 * </pre>
 */
public class LiveMicSageMakerExample {

    public static void main(String[] args) throws Exception {
        String endpointName = System.getenv().getOrDefault("SAGEMAKER_ENDPOINT", "deepgram-nova-3");
        String region = System.getenv().getOrDefault("AWS_REGION", "us-west-2");

        // Audio format: 16-bit PCM, 16kHz, mono
        float sampleRate = 16000;
        int sampleSizeInBits = 16;
        int channels = 1;
        AudioFormat format = new AudioFormat(sampleRate, sampleSizeInBits, channels, true, false);

        // Check for microphone
        DataLine.Info micInfo = new DataLine.Info(TargetDataLine.class, format);
        if (!AudioSystem.isLineSupported(micInfo)) {
            System.err.println("Microphone not available or format not supported.");
            System.exit(1);
        }

        // Create the SageMaker transport
        SageMakerConfig config = SageMakerConfig.builder()
                .endpointName(endpointName)
                .region(region)
                .build();
        SageMakerTransportFactory factory = new SageMakerTransportFactory(config);

        DeepgramClient client = DeepgramClient.builder()
                .apiKey("unused")
                .transportFactory(factory)
                .build();

        System.out.println("Live Microphone Transcription via SageMaker");
        System.out.println("Endpoint: " + endpointName);
        System.out.println("Region:   " + region);
        System.out.println("Audio:    " + (int) sampleRate + " Hz, " + sampleSizeInBits + "-bit, mono");
        System.out.println();

        V1WebSocketClient wsClient = client.listen().v1().v1WebSocket();
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean running = new AtomicBoolean(true);

        wsClient.onResults(result -> {
            if (result.getChannel() != null
                    && result.getChannel().getAlternatives() != null
                    && !result.getChannel().getAlternatives().isEmpty()) {
                ListenV1ResultsChannelAlternativesItem alt =
                        result.getChannel().getAlternatives().get(0);
                String transcript = alt.getTranscript();
                if (transcript != null && !transcript.isEmpty()) {
                    boolean isFinal = result.getIsFinal().orElse(false);
                    if (isFinal) {
                        System.out.println(transcript);
                    } else {
                        // Overwrite interim results on the same line
                        System.out.print("\r  ... " + transcript);
                    }
                }
            }
        });

        wsClient.onError(error -> {
            System.err.println("\nError: " + error.getMessage());
            running.set(false);
            done.countDown();
        });

        wsClient.onDisconnected(reason -> {
            System.out.println("\nDisconnected (code: " + reason.getCode() + ")");
            done.countDown();
        });

        // Connect with interim results enabled
        CompletableFuture<Void> connectFuture = wsClient.connect(
                V1ConnectOptions.builder()
                        .model(ListenV1Model.NOVA3)
                        .interimResults(ListenV1InterimResults.TRUE)
                        .encoding(ListenV1Encoding.LINEAR16)
                        .sampleRate(ListenV1SampleRate.of(16000))
                        .build());
        connectFuture.get(30, TimeUnit.SECONDS);

        System.out.println("Listening... speak into your microphone. Press Ctrl+C to stop.\n");

        // Shutdown hook for clean exit
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nStopping...");
            running.set(false);
        }));

        // Open microphone and stream audio
        TargetDataLine mic = (TargetDataLine) AudioSystem.getLine(micInfo);
        mic.open(format);
        mic.start();

        byte[] buffer = new byte[8192];
        while (running.get()) {
            int bytesRead = mic.read(buffer, 0, buffer.length);
            if (bytesRead > 0) {
                byte[] chunk = new byte[bytesRead];
                System.arraycopy(buffer, 0, chunk, 0, bytesRead);
                wsClient.sendMedia(ByteString.of(chunk));
            }
        }

        // Clean shutdown
        mic.stop();
        mic.close();

        wsClient.sendCloseStream(
                ListenV1CloseStream.builder()
                        .type(ListenV1CloseStreamType.CLOSE_STREAM)
                        .build());

        done.await(15, TimeUnit.SECONDS);
        wsClient.disconnect();
        factory.shutdown();
        System.out.println("Done.");
    }
}
