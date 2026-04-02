package com.deepgram.examples;

import com.deepgram.DeepgramClient;
import com.deepgram.resources.speak.v1.types.SpeakV1Close;
import com.deepgram.resources.speak.v1.types.SpeakV1CloseType;
import com.deepgram.resources.speak.v1.types.SpeakV1Flush;
import com.deepgram.resources.speak.v1.types.SpeakV1FlushType;
import com.deepgram.resources.speak.v1.types.SpeakV1Text;
import com.deepgram.resources.speak.v1.websocket.V1ConnectOptions;
import com.deepgram.resources.speak.v1.websocket.V1WebSocketClient;
import com.deepgram.sagemaker.SageMakerConfig;
import com.deepgram.sagemaker.SageMakerTransportFactory;
import com.deepgram.types.SpeakV1Model;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Streaming text-to-speech via a SageMaker endpoint.
 *
 * <p>Sends text to a Deepgram TTS model running on SageMaker and saves the
 * resulting audio to a WAV file.
 *
 * <p>Usage:
 * <pre>
 *   export SAGEMAKER_ENDPOINT=deepgram-tts-greg
 *   export AWS_REGION=us-east-2
 *   java -cp "examples/build/install/examples/lib/*" com.deepgram.examples.TtsSageMakerExample
 * </pre>
 */
public class TtsSageMakerExample {

    public static void main(String[] args) throws Exception {
        String endpointName = System.getenv().getOrDefault("SAGEMAKER_ENDPOINT", "deepgram-tts");
        String region = System.getenv().getOrDefault("AWS_REGION", "us-west-2");
        String outputFile = args.length > 0 ? args[0] : "tts_output.wav";

        SageMakerConfig config = SageMakerConfig.builder()
                .endpointName(endpointName)
                .region(region)
                .build();
        SageMakerTransportFactory factory = new SageMakerTransportFactory(config);

        DeepgramClient client = DeepgramClient.builder()
                .apiKey("unused")
                .transportFactory(factory)
                .build();

        System.out.println("Text-to-Speech via SageMaker");
        System.out.println("Endpoint: " + endpointName);
        System.out.println("Region:   " + region);
        System.out.println("Output:   " + outputFile);
        System.out.println();

        V1WebSocketClient wsClient = client.speak().v1().v1WebSocket();
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger audioChunks = new AtomicInteger(0);

        OutputStream audioOutput = new FileOutputStream(outputFile);

        wsClient.onSpeakV1Audio(audioData -> {
            try {
                byte[] bytes = audioData.toByteArray();
                audioOutput.write(bytes);
                int count = audioChunks.incrementAndGet();
                System.out.printf("Received audio chunk #%d (%d bytes)%n", count, bytes.length);
            } catch (Exception e) {
                System.err.println("Error writing audio: " + e.getMessage());
            }
        });

        wsClient.onFlushed(flushed -> {
            System.out.println("Flushed — all queued text has been converted");
        });

        wsClient.onError(error -> {
            System.err.println("Error: " + error.getMessage());
            done.countDown();
        });

        wsClient.onDisconnected(reason -> {
            try { audioOutput.close(); } catch (Exception e) { /* ignore */ }
            System.out.println("Closed (code: " + reason.getCode() + ")");
            done.countDown();
        });

        CompletableFuture<Void> connectFuture = wsClient.connect(
                V1ConnectOptions.builder()
                        .model(SpeakV1Model.AURA2ATLAS_EN)
                        .build());
        connectFuture.get(30, TimeUnit.SECONDS);
        System.out.println("Connected. Sending text...\n");

        String[] sentences = {
            "Hello, this is a text-to-speech test running on Amazon SageMaker.",
            "The Deepgram model is generating audio from text in real time.",
            "This audio is being streamed back through the Java SDK transport layer."
        };

        for (String sentence : sentences) {
            System.out.println("Sending: \"" + sentence + "\"");
            wsClient.sendText(SpeakV1Text.builder().text(sentence).build());
        }

        wsClient.sendFlush(
                SpeakV1Flush.builder().type(SpeakV1FlushType.FLUSH).build());

        // Wait for audio to arrive
        Thread.sleep(10000);

        wsClient.sendClose(
                SpeakV1Close.builder().type(SpeakV1CloseType.CLOSE).build());

        done.await(15, TimeUnit.SECONDS);
        wsClient.disconnect();
        factory.shutdown();

        System.out.printf("%nTotal audio chunks: %d%n", audioChunks.get());
        System.out.printf("Audio saved to %s%n", outputFile);

        // Play audio on macOS
        System.out.println("Playing audio...");
        new ProcessBuilder("afplay", outputFile).inheritIO().start().waitFor();

        System.out.println("Done.");
    }
}
