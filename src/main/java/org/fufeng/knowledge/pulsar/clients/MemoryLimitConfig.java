package org.fufeng.knowledge.pulsar.clients;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SizeUnit;

public class MemoryLimitConfig {

    public static void main(String[] args) throws Exception {
        // 使用内存限制参数来控制客户端内存使用总量，该客户端下的生产者和消费者将竞争分配的内存。
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://127.0.0.1:6650")
                .memoryLimit(64, SizeUnit.MEGA_BYTES)
                .build();
    }

}
