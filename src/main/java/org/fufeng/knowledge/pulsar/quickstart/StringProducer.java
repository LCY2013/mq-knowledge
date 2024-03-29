package org.fufeng.knowledge.pulsar.quickstart;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import java.util.Base64;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class StringProducer {

    public static void main(String[] args) throws Exception {
        PulsarClient client = Client();
        Producer<String> stringProducer = client.newProducer(Schema.STRING)
                .topic("my-topic")
                .create();
        for (int i = 0; i < 10;i ++) {
            MessageId myMessage = stringProducer.send("My message"+i);
            System.out.println(i + "message ID: " + Base64.getEncoder().encodeToString(myMessage.toByteArray()));
        }

        stringProducer.close();
        client.close();
    }

}
