package org.fufeng.knowledge.pulsar.reader;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class StickyKeyRangeReader {

    private static final Logger logger = LoggerFactory.getLogger(StickyKeyRangeReader.class);

    public static void main(String[] args) throws Exception {
        PulsarClient client = Client();

        /*
        在粘性密钥范围读取器中，代理仅调度消息密钥的哈希包含指定密钥哈希范围的消息。可以在读取器上指定多个密钥哈希范围。
        总哈希范围大小为 65536，因此范围的最大末端应小于或等于 65535。
         */
        client.newReader()
                .topic("my-topic")
                .startMessageId(MessageId.earliest)
                .keyHashRange(Range.of(0, 10000), Range.of(20001, 30000))
                .create();
    }

}
