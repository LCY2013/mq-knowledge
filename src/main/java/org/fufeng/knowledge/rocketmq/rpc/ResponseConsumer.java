package org.fufeng.knowledge.rocketmq.rpc;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.utils.MessageUtil;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class ResponseConsumer {
    public static void main(String[] args) throws InterruptedException, MQClientException {
        String producerGroup = "please_rename_unique_group_name";
        String consumerGroup = "please_rename_unique_group_name";
        String topic = "RequestTopic";

        // create a producer to send reply message
        DefaultMQProducer replyProducer = new DefaultMQProducer(producerGroup);
        replyProducer.setNamesrvAddr("127.0.0.1:9876");
        replyProducer.start();

        // create consumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setNamesrvAddr("127.0.0.1:9876");

        // recommend client configs
        consumer.setPullTimeDelayMillsWhenException(0L);

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
            for (MessageExt msg : msgs) {
                try {
                    System.out.printf("handle message: %s %n", msg.toString());
                    String replyTo = MessageUtil.getReplyToClient(msg);
                    byte[] replyContent = "reply message contents.".getBytes(StandardCharsets.UTF_8);
                    // create reply message with given util, do not create reply message by yourself
                    Message replyMessage = MessageUtil.createReplyMessage(msg, replyContent);

                    // send reply message with producer
                    SendResult replyResult = replyProducer.send(replyMessage, 3000);
                    System.out.printf("reply to %s , %s %n", replyTo, replyResult.toString());
                } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.subscribe(topic, "*");
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
