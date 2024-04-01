package org.fufeng.knowledge.pulsar.reader;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class TopicReader {

    private static final Logger logger = LoggerFactory.getLogger(TopicReader.class);

    public static void main(String[] args) throws Exception {
        /*
        读者只是一个没有光标的消费者。这意味着 Pulsar 不会跟踪进度，也无需确认消息。

        下面是一个从某个主题的最早可用消息开始读取的示例。
         */
        //ReadEarliest();
        //ReadLatest();
        ReadFromMessageId();
    }

    public static void ReadEarliest() throws Exception {
        PulsarClient client = Client();
        // Create a reader on a topic and for a specific message (and onward)
        Reader<byte[]> reader = client.newReader()
                .topic("my-topic")
                .startMessageId(MessageId.earliest)
                .create();

        while (true) {
            Message msg = reader.readNext();
            // Do something with the message
            logger.info("Read Message received: " + new String(msg.getData()));
            logger.info("Read Message properties: " + msg.getProperties());
            logger.info("Read Message key: " + msg.getKey());
            logger.info("Read Message ID: " + Base64.getEncoder().encodeToString(msg.getMessageId().toByteArray()));
            // Process the message
        }
    }

    public static void ReadLatest() throws Exception {
        PulsarClient client = Client();
        // Create a reader on a topic and for a specific message (and onward)
        Reader<byte[]> reader = client.newReader()
                .topic("my-topic")
                .startMessageId(MessageId.latest)
                .create();

        while (true) {
            Message msg = reader.readNext();
            // Do something with the message
            logger.info("Read Message received: " + new String(msg.getData()));
            logger.info("Read Message properties: " + msg.getProperties());
            logger.info("Read Message key: " + msg.getKey());
            logger.info("Read Message ID: " + Base64.getEncoder().encodeToString(msg.getMessageId().toByteArray()));
            // Process the message
        }
    }

    public static void ReadFromMessageId() throws Exception {
        PulsarClient client = Client();
        MessageId id = MessageId.fromByteArray(Base64.getDecoder().decode("CDoQGzAA"));
        // Create a reader on a topic and for a specific message (and onward)
        Reader<byte[]> reader = client.newReader()
                .topic("my-topic")
                .startMessageId(id)
                .create();

        while (true) {
            Message msg = reader.readNext();
            // Do something with the message
            logger.info("Read Message received: " + new String(msg.getData()));
            logger.info("Read Message properties: " + msg.getProperties());
            logger.info("Read Message key: " + msg.getKey());
            logger.info("Read Message ID: " + Base64.getEncoder().encodeToString(msg.getMessageId().toByteArray()));
            // Process the message
        }
    }

}
