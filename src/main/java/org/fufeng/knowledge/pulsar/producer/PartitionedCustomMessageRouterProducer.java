package org.fufeng.knowledge.pulsar.producer;

import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class PartitionedCustomMessageRouterProducer {

    private static final Logger logger = LoggerFactory.getLogger(PartitionedCustomMessageRouterProducer.class);

    public static void main(String[] args) throws Exception {
        PulsarClient pulsarClient = Client();

        //使用内置消息
        //路由模式决定每条消息应该发布到哪个分区（内部主题）
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("partition-topic")
                .messageRouter(new AlwaysTenRouter())
                .create();

        // 同步发送消息
        MessageId messageId = producer.newMessage()
                .value("custom-partition-route-message".getBytes(StandardCharsets.UTF_8))
                .send();

        logger.info("messageId : {}", Base64.getEncoder().encodeToString(messageId.toByteArray()));

        producer.close();
        pulsarClient.close();
    }

    public static class AlwaysTenRouter implements MessageRouter {
        public int choosePartition(Message msg) {
            return 5;
        }
    }

    /*public static class MessageWithKeyRouter implements MessageRouter {
        public int choosePartition(Message msg) {
            // If the message has a key, it supersedes the round robin routing policy
            if (msg.hasKey()) {
                return signSafeMod(hash.makeHash(msg.getKey()), topicMetadata.numPartitions());
            }

            if (isBatchingEnabled) { // if batching is enabled, choose partition on `partitionSwitchMs` boundary.
                long currentMs = clock.millis();
                return signSafeMod(currentMs / partitionSwitchMs + startPtnIdx, topicMetadata.numPartitions());
            } else {
                return signSafeMod(PARTITION_INDEX_UPDATER.getAndIncrement(this), topicMetadata.numPartitions());
            }
        }
    }*/
}
