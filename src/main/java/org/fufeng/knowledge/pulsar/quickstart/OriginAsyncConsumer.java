package org.fufeng.knowledge.pulsar.quickstart;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.tomcat.jni.Time;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class OriginAsyncConsumer {

    public static void main(String[] args) throws Exception {
        MessageListener myMessageListener = (consumer, msg) -> {
            try {
                System.out.println("Message received: " + new String(msg.getData()));
                consumer.acknowledge(msg);
            } catch (Exception e) {
                consumer.negativeAcknowledge(msg);
            }
        };

        PulsarClient client = Client();
        Consumer consumer = client.newConsumer()
                .topic("my-topic")
                .subscriptionName("my-subscription")
                .messageListener(myMessageListener)
                .subscribe();

        Thread.sleep(Time.sec(10));
        consumer.close();
        client.close();
    }

}
