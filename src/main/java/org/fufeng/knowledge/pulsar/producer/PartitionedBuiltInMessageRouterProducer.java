package org.fufeng.knowledge.pulsar.producer;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class PartitionedBuiltInMessageRouterProducer {

    private static final Logger logger = LoggerFactory.getLogger(PartitionedBuiltInMessageRouterProducer.class);

    public static void main(String[] args) throws Exception {
        PulsarClient pulsarClient = Client();

        //使用内置消息
        //路由模式决定每条消息应该发布到哪个分区（内部主题）
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("partition-topic")
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
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
