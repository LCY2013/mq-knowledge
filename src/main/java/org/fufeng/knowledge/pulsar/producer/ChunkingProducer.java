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

public class ChunkingProducer {

    private static final Logger logger = LoggerFactory.getLogger(AsyncProducer.class);

    public static void main(String[] args) throws Exception {
        //消息分块使 Pulsar 能够通过在生产者端将消息拆分为块并在消费者端聚合分块消息来处理大负载消息。
        //消息分块功能默认处于关闭状态。

        PulsarClient pulsarClient = Client();
        // 同步生产者
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("my-topic")
                .enableChunking(true)
                .enableBatching(false)
                .create();

        // 同步发送消息
        MessageId messageId = producer.newMessage()
                .value("my-sync-message")
                .send();

        logger.info("messageId : {}", Base64.getEncoder().encodeToString(messageId.toByteArray()));

        producer.close();
        pulsarClient.close();
    }

}
