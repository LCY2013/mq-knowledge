package org.fufeng.knowledge.rocketmq.schedule;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

public class TimerMessageConsumer {

    //Note: TimerMessage is a new feature in version 5.0, so be sure to upgrade RocketMQ to version 5.0+ before using it.

    public static final String CONSUMER_GROUP = "TimerMessageConsumerGroup";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "TimerTopic";

    public static void main(String[] args) throws Exception {
        // Instantiate message consumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);

        // Uncomment the following line while debugging, namesrvAddr should be set to your local address
//        consumer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

        // Subscribe topics
        consumer.subscribe(TOPIC, "*");
        // Register message listener
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
            for (MessageExt message : messages) {
                // Print approximate delay time period
                System.out.printf("Receive message[msgId=%s %d  ms later]\n", message.getMsgId(),
                        System.currentTimeMillis() - message.getBornTimestamp());
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        // Launch consumer
        consumer.start();
        //info:to see the time effect, run the consumer first , it will wait for the msg
        //then start the producer
    }
}
