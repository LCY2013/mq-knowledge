package org.fufeng.knowledge.pulsar.reader;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class ConfigReader {

    private static final Logger logger = LoggerFactory.getLogger(ConfigReader.class);

    public static void main(String[] args) throws Exception {
        //https://pulsar.apache.org/docs/3.2.x/client-libraries-readers/#configure-chunking
        PulsarClient client = Client();
        Reader<byte[]> reader = client.newReader()
                .topic("my-topic")
                .startMessageId(MessageId.earliest)
                .maxPendingChunkedMessage(12)
                .autoAckOldestChunkedMessageOnQueueFull(true)
                .expireTimeOfIncompleteChunkedMessage(12, TimeUnit.MILLISECONDS)
                .create();
    }

}
