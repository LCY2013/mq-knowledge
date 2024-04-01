package org.fufeng.knowledge.pulsar.consumer;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.MultiplierRedeliveryBackoff;

import java.util.concurrent.TimeUnit;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class SetParamsConsumer {

    public static void main(String[] args) throws Exception {

    }

    public static void Normal() throws Exception {
        PulsarClient client = Client();
        Consumer<byte[]> consumer = client.newConsumer()
                .topic("my-topic")
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        /*
        默认批量接收策略为：
        BatchReceivePolicy.builder()
        .maxNumMessage(-1)
        .maxNumBytes(10 * 1024 * 1024)
        .timeout(100, TimeUnit.MILLISECONDS)
        .build();
         */
        Messages messages = consumer.batchReceive();
        for (Object message : messages) {
            // do something
        }
        // 单独确认消息
        consumer.acknowledge(messages);
        // 累计确认消息
        //consumer.acknowledgeCumulative(messages);
    }

    public static void BatchParam() throws Exception {
        //批量接收策略限制单个批次中消息的数量和字节。
        // 您可以指定超时以等待足够的消息。
        // 满足以下任一条件即批量接收完成：消息数量、消息字节数、等待超时。
        PulsarClient client = Client();
        Consumer consumer = client.newConsumer()
                .topic("my-topic")
                .subscriptionName("my-subscription")
                .batchReceivePolicy(BatchReceivePolicy.builder()
                        .maxNumMessages(100)
                        .maxNumBytes(1024 * 1024)
                        .timeout(200, TimeUnit.MILLISECONDS)
                        .build())
                .subscribe();

        Messages messages = consumer.batchReceive();
        for (Object message : messages) {
            // do something
        }
        consumer.acknowledge(messages);
    }

    public static void RedeliveryBackoff() throws Exception {
        //RedeliveryBackoff了重新传递退避机制。通过设置消息重投次数，可以实现不同延迟的重投
        PulsarClient client = Client();
        Consumer consumer =  client.newConsumer()
                .topic("my-topic")
                .subscriptionName("my-subscription")
                .negativeAckRedeliveryBackoff(MultiplierRedeliveryBackoff.builder()
                        .minDelayMs(1000)
                        .maxDelayMs(60 * 1000)
                        .build())
                .subscribe();
    }

    public static void ackTimeoutRedeliveryBackoff() throws Exception {
        //RedeliveryBackoff了重新传递退避机制。通过设置消息重投次数，可以实现不同延迟的重投
        PulsarClient client = Client();
        Consumer consumer =  client.newConsumer()
                .topic("my-topic")
                .subscriptionName("my-subscription")
                .ackTimeout(10, TimeUnit.SECONDS)
                .ackTimeoutRedeliveryBackoff(MultiplierRedeliveryBackoff.builder()
                        .minDelayMs(1000)
                        .maxDelayMs(60000)
                        .multiplier(2)
                        .build())
                .subscribe();
        //上面消息重新传递行为应如下所示。
        //
        //再次投递计数	再次投递延迟
        //   1	          10+1秒
        //   2	          10+2秒
        //   3	          10+4秒
        //   4	          10+8秒
        //   5	          10 + 16 秒
        //   6	          10 + 32 秒
        //   7	          10+60秒
        //   8	          10+60秒

        //不起作用negativeAckRedeliveryBackoff，consumer.negativeAcknowledge(MessageId messageId)因为您无法从消息 ID 获取重新传递计数。
        //如果消费者崩溃，它会触发未确认消息的重新传递。在这种情况下，RedeliveryBackoff不会生效，并且消息可能会早于退避的延迟时间重新传送。
    }

    public static void Chunked() throws Exception {
        //可以通过配置特定参数来限制消费者同时维护的分块消息的最大数量。
        //当达到配置的阈值时，消费者通过静默确认消息或要求代理稍后重新传送消息来删除待处理消息
        PulsarClient client = Client();
        Consumer<byte[]> consumer = client.newConsumer()
                .topic("my-topic")
                .subscriptionName("test")
                .autoAckOldestChunkedMessageOnQueueFull(true)
                .maxPendingChunkedMessage(100)
                .expireTimeOfIncompleteChunkedMessage(10, TimeUnit.MINUTES)
                .subscribe();
    }

}
