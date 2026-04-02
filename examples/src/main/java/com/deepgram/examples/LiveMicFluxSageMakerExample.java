package com.deepgram.examples;

import com.deepgram.DeepgramClient;
import com.deepgram.resources.listen.v2.types.ListenV2CloseStream;
import com.deepgram.resources.listen.v2.types.ListenV2CloseStreamType;
import com.deepgram.resources.listen.v2.types.ListenV2TurnInfoEvent;
import com.deepgram.resources.listen.v2.websocket.V2ConnectOptions;
import com.deepgram.resources.listen.v2.websocket.V2WebSocketClient;
import com.deepgram.sagemaker.SageMakerConfig;
import com.deepgram.sagemaker.SageMakerTransportFactory;
import com.deepgram.types.ListenV2Encoding;
import com.deepgram.types.ListenV2SampleRate;

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
 * Live microphone transcription via SageMaker using Flux (Listen V2).
 *
 * <p>Captures audio from the system microphone and streams it to a Flux model
 * running on SageMaker. Uses the V2 turn-based transcription API.
 * Press Ctrl+C to stop.
 *
 * <p>Usage:
 * <pre>
 *   export SAGEMAKER_ENDPOINT=deepgram-flux-greg
 *   export AWS_REGION=us-east-2
 *   java -cp "examples/build/install/examples/lib/*" com.deepgram.examples.LiveMicFluxSageMakerExample
 * </pre>
 */
public class LiveMicFluxSageMakerExample {

    public static void main(String[] args) throws Exception {
        String endpointName = System.getenv().getOrDefault("SAGEMAKER_ENDPOINT", "deepgram-flux");
        String region = System.getenv().getOrDefault("AWS_REGION", "us-west-2");

        // Audio format: 16-bit PCM, 16kHz, mono
        float sampleRate = 16000;
        int sampleSizeInBits = 16;
        int channels = 1;
        AudioFormat format = new AudioFormat(sampleRate, sampleSizeInBits, channels, true, false);

        DataLine.Info micInfo = new DataLine.Info(TargetDataLine.class, format);
        if (!AudioSystem.isLineSupported(micInfo)) {
            System.err.println("Microphone not available or format not supported.");
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

        System.out.println("Live Microphone Transcription via SageMaker (Flux V2)");
        System.out.println("Endpoint: " + endpointName);
        System.out.println("Region:   " + region);
        System.out.println("Audio:    " + (int) sampleRate + " Hz, " + sampleSizeInBits + "-bit, mono");
        System.out.println();

        V2WebSocketClient wsClient = client.listen().v2().v2WebSocket();
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean running = new AtomicBoolean(true);

        wsClient.onTurnInfo(turnInfo -> {
            String transcript = turnInfo.getTranscript();
            ListenV2TurnInfoEvent event = turnInfo.getEvent();

            if (transcript != null && !transcript.isEmpty()) {
                if (event == ListenV2TurnInfoEvent.END_OF_TURN) {
                    System.out.println(transcript);
                } else {
                    System.out.print("\r  ... " + transcript);
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

        CompletableFuture<Void> connectFuture = wsClient.connect(
                V2ConnectOptions.builder()
                        .model("flux-general-en")
                        .encoding(ListenV2Encoding.LINEAR16)
                        .sampleRate(ListenV2SampleRate.of(16000))
                        .build());
        connectFuture.get(30, TimeUnit.SECONDS);

        System.out.println("Listening... speak into your microphone. Press Ctrl+C to stop.\n");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nStopping...");
            running.set(false);
        }));

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

        mic.stop();
        mic.close();

        wsClient.sendCloseStream(
                ListenV2CloseStream.builder()
                        .type(ListenV2CloseStreamType.CLOSE_STREAM)
                        .build());

        done.await(15, TimeUnit.SECONDS);
        wsClient.disconnect();
        factory.shutdown();
        System.out.println("Done.");
    }
}
