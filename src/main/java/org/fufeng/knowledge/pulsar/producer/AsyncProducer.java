package org.fufeng.knowledge.pulsar.producer;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class AsyncProducer {

    private static final Logger logger = LoggerFactory.getLogger(AsyncProducer.class);

    public static void main(String[] args) {
        PulsarClient pulsarClient = Client();

        // 异步生产者
        pulsarClient.newProducer(Schema.STRING)
                .topic("my-topic")
                .createAsync()
                .thenAccept(producer -> {
                    logger.info("Producer created: {}", producer.getProducerName());

                    // 异步发送
                    producer.newMessage()
                            .value("my-sync-message")
                            .sendAsync()
                            .thenAccept(messageId -> {
                                logger.info("messageId : {}", Base64.getEncoder().encodeToString(messageId.toByteArray()));
                            });
                });
    }

}
