package org.fufeng.knowledge.rocketmq.rpc;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.RequestCallback;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class AsyncRequestProducer {
    private static final Logger log = LoggerFactory.getLogger(AsyncRequestProducer.class);

    public static void main(String[] args) throws MQClientException, InterruptedException {
        String producerGroup = "please_rename_unique_group_name";
        String topic = "RequestTopic";
        long ttl = 3000;

        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        producer.start();

        try {
            Message msg = new Message(topic,
                    "",
                    "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));

            long begin = System.currentTimeMillis();
            producer.request(msg, new RequestCallback() {
                @Override
                public void onSuccess(Message message) {
                    long cost = System.currentTimeMillis() - begin;
                    System.out.printf("request to <%s> cost: %d replyMessage: %s %n", topic, cost, message);
                }

                @Override
                public void onException(Throwable e) {
                    System.err.printf("request to <%s> fail.", topic);
                }
            }, ttl);
        } catch (Exception e) {
            log.warn("", e);
        }
        /* shutdown after your request callback is finished */
//        producer.shutdown();
    }
}