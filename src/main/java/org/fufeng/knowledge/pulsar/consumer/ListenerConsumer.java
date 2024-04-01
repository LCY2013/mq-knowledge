package org.fufeng.knowledge.pulsar.consumer;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class ListenerConsumer {

    public static void main(String[] args) throws Exception {
        // 您可以通过使用为收到的每条消息调用的消息侦听器来阻止基于事件样式的调用，从而避免运行循环
        PulsarClient client = Client();
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic")
                .subscriptionName("my-subscription")
                .messageListener((c, m) -> {
                    try {
                        c.acknowledge(m);
                    } catch (Exception e) {
                        //Assert.fail("Failed to acknowledge", e);
                    }
                }).subscribe();
    }

}
