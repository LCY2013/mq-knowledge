package org.fufeng.knowledge.pulsar.consumer;

import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class KeySharedConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KeySharedConsumer.class);

    private static final PulsarClient pulsarClient = Client();

    public static void main(String[] args) throws Exception {
        //conumser1 is the active consumer, consumer2 is the standby consumer.
        //consumer1 receives 5 messages and then crashes, consumer2 takes over as an  active consumer.
        ExecutorService executorService = Executors.newFixedThreadPool(2, Thread::new);
        executorService.submit(() -> {
            KeySharedConsumer.run("consumer1");
        });
        executorService.submit(() -> {
            KeySharedConsumer.run("consumer2");
        });
        boolean b = executorService.awaitTermination(300, TimeUnit.SECONDS);
        logger.info("exec {}", b);
        pulsarClient.close();
    }

    public static void run(String consumerName) {
        try (Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic("key-shared-topic")
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe()){

            while (true) {
                // Wait for a message
                Message<String> msg = consumer.receive();

                try {
                    // Do something with the message
                    logger.info(consumerName + " Message received: " + new String(msg.getData()));
                    logger.info(consumerName + " Message properties: " + msg.getProperties());
                    logger.info(consumerName + " Message key: " + msg.getKey());

                    // Acknowledge the message
                    consumer.acknowledge(msg);
                } catch (Exception e) {
                    // Message failed to process, redeliver later
                    consumer.negativeAcknowledge(msg);
                }
            }
        } catch (Exception e) {
            logger.error("err: ", e);
        }
    }

}
