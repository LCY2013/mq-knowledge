package org.fufeng.knowledge.pulsar.admin;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.BrokerInfo;
import org.apache.pulsar.common.policies.data.NamespaceOwnershipStatus;
import org.fufeng.knowledge.pulsar.consumer.ExclusiveConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.fufeng.knowledge.pulsar.common.Common.Admin;
import static org.fufeng.knowledge.pulsar.common.Common.ClusterName;

public class AdminBrokers {

    private static final Logger logger = LoggerFactory.getLogger(ExclusiveConsumer.class);

    private static final PulsarAdmin admin = Admin();

    public static void main(String[] args) throws Exception {
        //activeBrokers();
        //ownedNamespaces();
        //dynamicConfigurationNames();
        //updateDynamicConfiguration();
        //getAllDynamicConfigurations();
        getLeaderBroker();

        admin.close();
    }

    private static void getLeaderBroker() throws PulsarAdminException {
        BrokerInfo leaderBroker = admin.brokers().getLeaderBroker();
        logger.info("leader broker info: {}, {}", leaderBroker.getBrokerId(), leaderBroker.getServiceUrl());
    }

    private static void getAllDynamicConfigurations() throws PulsarAdminException {
        // 获取已动态更新的所有参数的列表
        // pulsar-admin brokers get-all-dynamic-config
        Map<String, String> allDynamicConfigurations = admin.brokers().getAllDynamicConfigurations();
        allDynamicConfigurations.forEach((k,v) -> logger.info("config name: {}, value: {}", k, v));
    }

    private static void updateDynamicConfiguration() throws PulsarAdminException {
        // 动态更新代理配置
        // pulsar-admin brokers update-dynamic-config --config brokerShutdownTimeoutMs --value 100
        admin.brokers().updateDynamicConfiguration("brokerShutdownTimeoutMs", "100");
    }

    private static void dynamicConfigurationNames() throws PulsarAdminException {
        // 获取所有可能可更新的配置参数的列表。
        // 您可以使用以下方式之一更新代理配置：
        //
        //启动代理时提供配置。
        //
        //运行代理时动态更新配置。
        //
        //由于 Pulsar 中的所有 Broker 配置都存储在 ZooKeeper 中，因此配置值也可以在 Broker 运行时动态更新。当您动态更新代理配置时，ZooKeeper 将通知代理更改，然后代理将覆盖任何现有的配置值。
        //pulsar-admin brokers list-dynamic-config
        List<String> dynamicConfigurationNames = admin.brokers().getDynamicConfigurationNames();
        dynamicConfigurationNames.forEach(config -> logger.info("config name: {}", config));
    }

    private static void ownedNamespaces() throws PulsarAdminException {
        //列出给定代理拥有和服务的所有命名空间
        //pulsar-admin brokers list use
        Map<String, NamespaceOwnershipStatus> ownedNamespaces = admin.brokers().getOwnedNamespaces(ClusterName, "broker:8080");
        ownedNamespaces.forEach((k,v) -> logger.info("owned namespaces k: {}, v: {}", k, v));
    }

    private static void activeBrokers() throws PulsarAdminException {
        //获取所有可用的活动代理，这些代理正在为具有集群名称的流量提供服务
        //pulsar-admin brokers list use
        List<String> activeBrokers =
                admin.brokers().getActiveBrokers(ClusterName);
        activeBrokers.forEach(broker -> logger.info("broker name: {}", broker));
    }

}
