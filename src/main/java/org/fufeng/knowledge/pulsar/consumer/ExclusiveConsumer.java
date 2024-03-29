package org.fufeng.knowledge.pulsar.consumer;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class ExclusiveConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ExclusiveConsumer.class);

    public static void main(String[] args) throws Exception {
        PulsarClient client = Client();
        Consumer<byte[]> consumer = client.newConsumer()
                .topic("my-topic")
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        while (true) {
            // Wait for a message
            Message msg = consumer.receive();

            try {
                // Do something with the message
                logger.info("Message received: " + new String(msg.getData()));
                logger.info("Message properties: " + msg.getProperties());
                logger.info("Message key: " + msg.getKey());

                // Acknowledge the message
                consumer.acknowledge(msg);
            } catch (Exception e) {
                // Message failed to process, redeliver later
                consumer.negativeAcknowledge(msg);
            }
        }
    }

}
