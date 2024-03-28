package org.fufeng.knowledge.rocketmq.common;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.TransactionChecker;

public class Common {

    // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8081;xxx:8081。
    public static final String endpoint = "172.16.49.207:8081";

    public static Producer getProducer(String ...topic) throws ClientException {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().setEndpoints(endpoint);
        ClientConfiguration configuration = builder.build();
        // 初始化Producer时需要设置通信配置以及预绑定的Topic。
        return provider.newProducerBuilder()
                .setTopics(topic)
                .setClientConfiguration(configuration)
                .build();
    }

    public static Producer getTxProducer(TransactionChecker checker, String ...topic) throws ClientException {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().setEndpoints(endpoint);
        ClientConfiguration configuration = builder.build();
        // 初始化Producer时需要设置通信配置以及预绑定的Topic。
        return provider.newProducerBuilder()
                .setTopics(topic)
                .setClientConfiguration(configuration)
                .setTransactionChecker(checker)
                .build();
    }

}
