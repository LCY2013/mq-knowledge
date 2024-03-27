package org.fufeng.knowledge.rocketmq.delay;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class DelaySimpleConsumer {

    private static final Logger logger = LoggerFactory.getLogger(DelayPushConsumer.class);

    public static void main(String[] args) {
        //消费示例二：使用SimpleConsumer消费定时消息，主动获取消息进行消费处理并提交消费结果。
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8081;xxx:8081。
        String endpoints = "localhost:8081";
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints(endpoints)
                .build();
        // 订阅消息的过滤规则，表示订阅所有Tag的消息。
        String tag = "*";
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        // 为消费者指定所属的消费者分组，Group需要提前创建。
        String consumerGroup = "delayTopic";
        // 指定需要订阅哪个目标Topic，Topic需要提前创建。
        String topic = "delayTopic";

        List<MessageView> messageViewList;
        try (SimpleConsumer simpleConsumer = provider.newSimpleConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                // 设置消费者分组。
                .setConsumerGroup(consumerGroup)
                // 设置预绑定的订阅关系。
                .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                .setAwaitDuration(Duration.ofSeconds(300))
                .build()) {
            messageViewList = simpleConsumer.receive(10, Duration.ofSeconds(30));
            messageViewList.forEach(messageView -> {
                logger.info("Consume message successfully, messageId={}, content={}", messageView.getMessageId(), messageView);
                //消费处理完成后，需要主动调用ACK提交消费结果。
                try {
                    simpleConsumer.ack(messageView);
                } catch (ClientException e) {
                    //如果遇到系统流控等原因造成拉取失败，需要重新发起获取消息请求。
                    logger.error("simpleConsumer.ack err:", e);
                }
            });
        } catch (Exception e) {
            //如果遇到系统流控等原因造成拉取失败，需要重新发起获取消息请求。
            logger.error("SimpleConsumer01 err:", e);
        }
    }

}
