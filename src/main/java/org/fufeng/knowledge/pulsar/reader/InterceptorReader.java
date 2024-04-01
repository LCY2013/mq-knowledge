package org.fufeng.knowledge.pulsar.reader;

import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class InterceptorReader {

    private static final Logger logger = LoggerFactory.getLogger(InterceptorReader.class);

    public static void main(String[] args) throws Exception {
        //Pulsar 阅读器拦截器会在Pulsar 阅读器读取消息之前通过用户定义的处理来拦截并可能改变消息。
        // 通过阅读器拦截器，您可以在读取消息之前应用统一的消息传递过程，例如修改消息、添加属性、收集统计信息等，而无需分别创建类似的机制。
        /*
        Pulsar 阅读器拦截器在 Pulsar 消费者拦截器之上工作。插件接口ReaderInterceptor可以被视为一个子集ConsumerInterceptor，它有两个主要事件。

        beforeRead在读者阅读消息之前触发。您可以修改此事件中的消息。
        onPartitionsChange当检测到分区上的更改时触发。
        要感知触发事件并进行自定义处理，可以ReaderInterceptor在创建时添加Reader如下。
         */
        PulsarClient client = Client();
        Reader<byte[]> reader = client.newReader()
                .topic("t1")
                .autoUpdatePartitionsInterval(5, TimeUnit.SECONDS)
                .intercept(new ReaderInterceptor<>() {
                    @Override
                    public void close() {
                    }

                    @Override
                    public Message<byte[]> beforeRead(Reader<byte[]> reader, Message<byte[]> message) {
                        // user-defined processing logic
                        return message;
                    }

                    @Override
                    public void onPartitionsChange(String topicName, int partitions) {
                        // user-defined processing logic
                    }
                })
                .startMessageId(MessageId.earliest)
                .create();
    }
}
