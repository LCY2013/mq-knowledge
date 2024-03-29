package org.fufeng.knowledge.pulsar.quickstart;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class FromMessageIdRead {

    public static void main(String[] args) throws Exception {
        PulsarClient client = Client();
        // 不包括指定的message id
        byte[] msgIdBytes = Base64.getDecoder().decode("CBsQFTAA"); // Some message ID byte array
        MessageId id = MessageId.fromByteArray(msgIdBytes);
        Reader reader = client.newReader()
                .topic("my-topic")
                .startMessageId(id)
                .create();

        while (true) {
            Message message = reader.readNext();
            // Process message
            System.out.println("Message received: " + new String(message.getData()));
        }

        //reader.close();
        //client.close();
    }

}
