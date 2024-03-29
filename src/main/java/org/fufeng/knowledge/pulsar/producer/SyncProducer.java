package org.fufeng.knowledge.pulsar.producer;

import com.google.common.collect.Lists;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class SyncProducer {

    private static final Logger logger = LoggerFactory.getLogger(AsyncProducer.class);


    public static void main(String[] args) throws Exception {
        PulsarClient pulsarClient = Client();
        // 同步生产者
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("my-topic")
                .create();

        // 同步发送消息
        MessageId messageId = producer.newMessage()
                .value("my-sync-message")
                .send();

        logger.info("messageId : {}", Base64.getEncoder().encodeToString(messageId.toByteArray()));

        // 配置相关发送消息属性
        messageId = producer.newMessage()
                .key("my-key") // Set the message key
                .eventTime(System.currentTimeMillis()) // Set the event time
                .sequenceId(1203) // Set the sequenceId for the deduplication purposes
                .deliverAfter(1, TimeUnit.HOURS) // Delay message delivery for 1 hour
                .property("my-key", "my-value") // Set the customized metadata
                .property("my-other-key", "my-other-value")
                .replicationClusters(
                        Lists.newArrayList("cluster-a", "cluster-a")) // Set the geo-replication clusters for this message.
                .value("content")
                .send();
        logger.info("messageId : {}", Base64.getEncoder().encodeToString(messageId.toByteArray()));

        // 用loadConf配置消息元数据
        Map<String, Object> conf = new HashMap<>();
        conf.put("key", "my-key");
        conf.put("eventTime", System.currentTimeMillis());
        messageId = producer.newMessage()
                .value("my-message")
                .loadConf(conf)
                .send();
        logger.info("messageId : {}", Base64.getEncoder().encodeToString(messageId.toByteArray()));

        producer.close();
        pulsarClient.close();
    }

}
