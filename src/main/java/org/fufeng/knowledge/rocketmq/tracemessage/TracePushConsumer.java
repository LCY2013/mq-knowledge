package org.fufeng.knowledge.rocketmq.tracemessage;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

public class TracePushConsumer {

    public static final String CONSUMER_GROUP = "ProducerGroupName";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "TopicTest";

    public static void main(String[] args) throws InterruptedException, MQClientException {
        // Here,we use the default message track trace topic name
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP, true, null);

        // Uncomment the following line while debugging, namesrvAddr should be set to your local address
//        consumer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        consumer.subscribe(TOPIC, "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // Wrong time format 2017_0422_221800
        consumer.setConsumeTimestamp("20181109221800");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
