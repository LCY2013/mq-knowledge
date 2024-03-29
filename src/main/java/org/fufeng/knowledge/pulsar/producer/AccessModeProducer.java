package org.fufeng.knowledge.pulsar.producer;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class AccessModeProducer {

    private static final Logger logger = LoggerFactory.getLogger(AccessModeProducer.class);

    public static void main(String[] args) throws Exception {
        // 访问模式允许应用程序要求对某个主题进行独占生产者访问，以实现“单写入者”情况。

        PulsarClient pulsarClient = Client();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("my-topic")
                .accessMode(ProducerAccessMode.Exclusive)
                .create();

        // 同步发送消息
        MessageId messageId = producer.newMessage()
                .value("my-sync-message".getBytes(StandardCharsets.UTF_8))
                .send();

        logger.info("messageId : {}", Base64.getEncoder().encodeToString(messageId.toByteArray()));

        producer.close();
        pulsarClient.close();
    }

}
