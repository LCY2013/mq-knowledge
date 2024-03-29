package org.fufeng.knowledge.pulsar.quickstart;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;

import java.util.Base64;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class OriginProducer {

    public static void main(String[] args) throws Exception {
        PulsarClient client = Client();
        Producer<byte[]> producer = client.newProducer()
                .topic("my-topic")
                .create();

        // You can then send messages to the broker and topic you specified:
        MessageId myMessage = producer.send("My message".getBytes());
        System.out.println("message ID: " + Base64.getEncoder().encodeToString(myMessage.toByteArray()));
        producer.closeAsync()
                .thenRun(() -> System.out.println("Producer closed"))
                .exceptionally((ex) -> {
                    System.err.println("Failed to close producer: " + ex);
                    return null;
                });
        client.close();
    }

}
