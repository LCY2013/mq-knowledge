package org.fufeng.knowledge.pulsar.clients;

import org.apache.pulsar.client.api.PulsarClient;

public class AdvertisingListenersConfig {

    public static void main(String[] args) throws Exception {
        // Pulsar 客户端如何使用多个通告的侦听器
        // 1. 在代理配置文件中配置多个通告的侦听器。
        // advertisedListeners={listenerName}:pulsar://xxxx:6650,{listenerName}:pulsar+ssl://xxxx:6651
        // eg: advertisedListeners=inner:pulsar://xxxx:6650,external:pulsar+ssl://xxxx:6651

        // 2. 为了确保内部和外部网络中的客户端都可以连接到 Pulsar 集群，Pulsar 引入了AdvertisingListeners。
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://127.0.0.1:6650")
                .listenerName("external")
                .build();
    }

}
