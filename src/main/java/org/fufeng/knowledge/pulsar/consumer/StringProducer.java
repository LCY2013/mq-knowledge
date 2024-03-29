package org.fufeng.knowledge.pulsar.consumer;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class StringProducer {

    private static final Logger logger = LoggerFactory.getLogger(StringProducer.class);

    public static void main(String[] args) throws Exception {
        PulsarClient pulsarClient = Client();

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("my-topic")
                .enableBatching(false)
                .create();
        // 3 messages with "key-1", 3 messages with "key-2", 2 messages with "key-3" and 2 messages with "key-4"
        producer.newMessage().key("key-1").value("message-1-1").send();
        producer.newMessage().key("key-1").value("message-1-2").send();
        producer.newMessage().key("key-1").value("message-1-3").send();
        producer.newMessage().key("key-2").value("message-2-1").send();
        producer.newMessage().key("key-2").value("message-2-2").send();
        producer.newMessage().key("key-2").value("message-2-3").send();
        producer.newMessage().key("key-3").value("message-3-1").send();
        producer.newMessage().key("key-3").value("message-3-2").send();
        producer.newMessage().key("key-4").value("message-4-1").send();
        producer.newMessage().key("key-4").value("message-4-2").send();

        producer.close();
        pulsarClient.close();
    }

}
