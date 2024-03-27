package org.fufeng.knowledge.rocketmq.normal;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.java.message.MessageBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.fufeng.knowledge.rocketmq.common.Common.getProducer;

public class Producer01 {
    private static final Logger logger = LoggerFactory.getLogger(PushConsumer01.class);

    public static void main(String[] args) throws Exception {
        Producer producer = getProducer();
        //普通消息发送。
        MessageBuilder messageBuilder = new MessageBuilderImpl();
        Message message = messageBuilder.setTopic("Producer01")
                //设置消息索引键，可根据关键字精确查找某条消息。
                .setKeys("Producer01")
                //设置消息Tag，用于消费端根据指定Tag过滤消息。
                .setTag("Producer01")
                //消息体。
                .setBody("messageBody".getBytes())
                .build();
        try {
            //发送消息，需要关注发送结果，并捕获失败等异常。
            SendReceipt sendReceipt = producer.send(message);
            System.out.println(sendReceipt.getMessageId());
        } catch (ClientException e) {
            logger.error("Producer01 err:", e);
        }
    }

}
