package org.fufeng.knowledge.rocketmq.delay;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.java.message.MessageBuilderImpl;
import org.fufeng.knowledge.rocketmq.normal.NormalPushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.fufeng.knowledge.rocketmq.common.Common.getProducer;

/**
 * 创建延时队列
 * sh bin/mqadmin updateTopic -n 127.0.0.1:9876 -t delayTopic -c DefaultCluster -a +message.type=DELAY
 */

public class DelayProducer {
    private static final Logger logger = LoggerFactory.getLogger(NormalPushConsumer.class);

    public static void main(String[] args) throws ClientException, IOException {
        Producer producer = getProducer();
        //定时/延时消息发送
        MessageBuilder messageBuilder = new MessageBuilderImpl();;
        //以下示例表示：延迟时间为10分钟之后的Unix时间戳。
        //long deliverTimeStamp = System.currentTimeMillis() + 10L * 60 * 1000;
        long deliverTimeStamp = System.currentTimeMillis() + 60 * 1000;
        Message message = messageBuilder.setTopic("delayTopic")
                //设置消息索引键，可根据关键字精确查找某条消息。
                .setKeys("messageKey")
                //设置消息Tag，用于消费端根据指定Tag过滤消息。
                .setTag("messageTag")
                .setDeliveryTimestamp(deliverTimeStamp)
                //消息体
                .setBody("messageBody".getBytes())
                .build();
        try {
            //发送消息，需要关注发送结果，并捕获失败等异常。
            SendReceipt sendReceipt = producer.send(message);
            System.out.println(sendReceipt.getMessageId());
        } catch (ClientException e) {
            logger.error("", e);
        } finally {
            producer.close();
        }
    }

}
