package org.fufeng.knowledge.pulsar.quickstart;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class OriginConsumer {

    public static void main(String[] args) throws Exception {
        PulsarClient client = Client();
        Consumer consumer = client.newConsumer()
                .topic("my-topic")
                .subscriptionName("my-subscription")
                .subscribe();

        while (true) {
            // Wait for a message
            Message msg = consumer.receive();

            try {
                // Do something with the message
                System.out.println("Message received: " + new String(msg.getData()));

                // Acknowledge the message
                consumer.acknowledge(msg);
            } catch (Exception e) {
                // Message failed to process, redeliver later
                consumer.negativeAcknowledge(msg);
            }
        }

        //consumer.close();
        //consumer.close();
    }

}
