package org.fufeng.knowledge.pulsar.consumer;

import org.apache.pulsar.client.api.*;

import java.util.Set;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class InterceptorConsumer {

    public static void main(String[] args) throws Exception {
        /*
        ConsumerInterceptor拦截并可能改变消费者收到的消息。
        该界面有六个主要事件：
        beforeConsumereceive()在或返回消息之前触发receiveAsync()。您可以修改此事件中的消息。
        onAcknowledge在消费者向代理发送确认之前触发。
        onAcknowledgeCumulative在消费者向代理发送累积确认之前触发。
        onNegativeAcksSend当发生否定确认的重新传送时会触发。
        onAckTimeoutSend当发生确认超时重新传送时触发。
        onPartitionsChange当（已分区）主题的分区发生变化时触发。
        要拦截消息，可以ConsumerInterceptor在创建时添加一个或多个 s Consumer，如下所示。
         */
        PulsarClient client = Client();
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic("my-topic")
                .subscriptionName("my-subscription")
                .intercept(new ConsumerInterceptor<>() {
                    @Override
                    public void close() {

                    }

                    @Override
                    public Message<String> beforeConsume(Consumer<String> consumer, Message<String> message) {
                        return null;
                    }

                    @Override
                    public void onAcknowledge(Consumer<String> consumer, MessageId messageId, Throwable exception) {

                    }

                    @Override
                    public void onAcknowledgeCumulative(Consumer<String> consumer, MessageId messageId, Throwable exception) {

                    }

                    @Override
                    public void onNegativeAcksSend(Consumer<String> consumer, Set<MessageId> messageIds) {

                    }

                    @Override
                    public void onAckTimeoutSend(Consumer<String> consumer, Set<MessageId> messageIds) {

                    }

                }).subscribe();
        //如果您使用多个拦截器，它们将按照传递给方法的顺序应用intercept
    }

}
