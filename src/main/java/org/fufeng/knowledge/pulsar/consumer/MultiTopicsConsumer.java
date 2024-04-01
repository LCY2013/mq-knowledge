package org.fufeng.knowledge.pulsar.consumer;

import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class MultiTopicsConsumer {
    private static final Logger logger = LoggerFactory.getLogger(MultiTopicsConsumer.class);

    private static final PulsarClient pulsarClient = Client();

    public static void main(String[] args) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(20, Thread::new);
        executorService.submit(MultiTopicsConsumer::run1);
        executorService.submit(MultiTopicsConsumer::run2);
        executorService.submit(MultiTopicsConsumer::run3);
        executorService.submit(MultiTopicsConsumer::run4);
        executorService.submit(MultiTopicsConsumer::run5);
        boolean b = executorService.awaitTermination(300, TimeUnit.SECONDS);
        logger.info("exec {}", b);
        pulsarClient.close();
    }

    public static void run1() {
        final ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName("multi-topic-all");
        // Subscribe to all topics in a namespace
        Pattern allTopicsInNamespace = Pattern.compile("public/default/.*");
        try (Consumer<String> consumer = consumerBuilder
                .topicsPattern(allTopicsInNamespace)
                .subscribe()) {
            while (true) {
                // Wait for a message
                Message<String> msg = consumer.receive();

                try {
                    // Do something with the message
                    logger.info("all consumer Message received: " + new String(msg.getData()));
                    logger.info("all consumer Message properties: " + msg.getProperties());
                    logger.info("all consumer Message key: " + msg.getKey());

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

    public static void run2() {
        final ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName("multi-topic-shared");
        // Subscribe to a subsets of topics in a namespace, based on regex
        Pattern someTopicsInNamespace = Pattern.compile("public/default/shared.*");
        try (Consumer consumer = consumerBuilder
                .topicsPattern(someTopicsInNamespace)
                .subscribe()) {
            while (true) {
                // Wait for a message
                Message<String> msg = consumer.receive();

                try {
                    // Do something with the message
                    logger.info("shared consumer Message received: " + new String(msg.getData()));
                    logger.info("shared consumer Message properties: " + msg.getProperties());
                    logger.info("shared consumer Message key: " + msg.getKey());

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

    /**
     * 订阅包括非持久化的主题
     */
    public static void run3() {
        Pattern pattern = Pattern.compile("public/default/.*");
        try (Consumer consumer = pulsarClient.newConsumer()
                .subscriptionName("multi-topic-all-with-non-persistent")
                .topicsPattern(pattern)
                .subscriptionTopicsMode(RegexSubscriptionMode.AllTopics)
                .subscribe()) {
            while (true) {
                // Wait for a message
                Message<String> msg = consumer.receive();

                try {
                    // Do something with the message
                    logger.info("with non-persistent consumer Message received: " + new String(msg.getData()));
                    logger.info("with non-persistent consumer Message properties: " + msg.getProperties());
                    logger.info("with non-persistent consumer Message key: " + msg.getKey());

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

    /**
     * 订阅指定的多个主题
     */
    public static void run4() {
        try (Consumer consumer = pulsarClient.newConsumer()
                .subscriptionName("multi-topic-all-with-topic-names")
                .topic("shared-topic", "key-shared-topic")
                .subscribe()) {
            while (true) {
                // Wait for a message
                Message<String> msg = consumer.receive();

                try {
                    // Do something with the message
                    logger.info("topic names consumer Message received: " + new String(msg.getData()));
                    logger.info("topic names consumer Message properties: " + msg.getProperties());
                    logger.info("topic names consumer Message key: " + msg.getKey());

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

    /**
     * 异步订阅主题
     */
    public static void run5() {
//        Pattern allTopicsInNamespace = Pattern.compile("persistent://public/default/.*");
        Pattern allTopicsInNamespace = Pattern.compile("public/default/.*");
        final ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName("multi-topic-all-async");

        consumerBuilder.topicsPattern(allTopicsInNamespace)
                .subscribeAsync()
                .thenAccept(MultiTopicsConsumer::receiveMessageFromConsumer);
    }

    private static void receiveMessageFromConsumer(Consumer<String> consumer) {
        while (true) {
            consumer.receiveAsync().thenAccept(message -> {
                // Do something with the received message
                //receiveMessageFromConsumer(consumer);
                logger.info("async consumer Message received: " + new String(message.getData()));
                logger.info("async consumer Message properties: " + message.getProperties());
                logger.info("async consumer Message key: " + message.getKey());
                // Acknowledge the message
                try {
                    consumer.acknowledge(message);
                } catch (PulsarClientException e) {
                    consumer.negativeAcknowledge(message);
                    throw new RuntimeException(e);
                }
            });
            try {
                consumer.unsubscribe();
            } catch (PulsarClientException e) {
                break;
            }
        }
    }
}


