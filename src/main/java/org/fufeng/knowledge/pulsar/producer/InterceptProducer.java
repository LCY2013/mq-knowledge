package org.fufeng.knowledge.pulsar.producer;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class InterceptProducer {

    private static final Logger logger = LoggerFactory.getLogger(InterceptProducer.class);

    public static void main(String[] args) throws Exception {
        //ProducerInterceptor在消息发布到代理之前拦截并可能改变生产者收到的消息。
        //eligible检查拦截器是否可以应用于消息。
        //beforeSend在生产者将消息发送给代理之前触发。您可以修改此事件中的消息。
        //onSendAcknowledgement当消息被broker确认或者发送失败时触发。

        //多个拦截器按照它们传递给方法的顺序应用intercept

        PulsarClient pulsarClient = Client();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("my-topic")
                .intercept(new ProducerInterceptor() {

                    @Override
                    public void close() {

                    }

                    @Override
                    public boolean eligible(Message message) {
                        return true;  // process all messages
                    }

                    @Override
                    public Message beforeSend(Producer producer, Message message) {
                        // user-defined processing logic
                        logger.info("before send message: {}", message);
                        return message;
                    }

                    @Override
                    public void onSendAcknowledgement(Producer producer, Message message, MessageId msgId, Throwable exception) {
                        // user-defined processing logic
                        logger.info("send ack: {}, {}", message, Base64.getEncoder().encodeToString(msgId.toByteArray()));
                    }
                }).create();


        // 同步发送消息
        MessageId messageId = producer.newMessage()
                .value("my-sync-message".getBytes(StandardCharsets.UTF_8))
                .send();

        logger.info("messageId : {}", Base64.getEncoder().encodeToString(messageId.toByteArray()));

        producer.close();
        pulsarClient.close();
    }

}
