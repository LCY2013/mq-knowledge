package org.fufeng.knowledge.pulsar.admin;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.*;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.fufeng.knowledge.pulsar.common.Common.Admin;

public class AdminNamespaces {

    private static final Logger logger = LoggerFactory.getLogger(AdminNamespaces.class);

    private static final PulsarAdmin admin = Admin();

    private static final String TENANT = "public";

    private static final String NAMESPACE = "public/test-namespace";

    public static void main(String[] args) {
        try {
            //createNamespace();
            //getPolicies();
            //getNamespaces();
            //deleteNamespace();
            //setNamespaceReplicationClusters();
            //getNamespaceReplicationClusters();
            //setBacklogQuota();
            //getBacklogQuotaMap();
            //removeBacklogQuota();
            //setPersistence();
            //getPersistence();
            //unloadNamespaceBundle();
            //splitNamespaceBundle();
            //setNamespaceMessageTTL();
            //getNamespaceMessageTTL();
            //removeNamespaceMessageTTL();
            //clearNamespaceBacklogForSubscription();
            //clearNamespaceBundleBacklogForSubscription();
            //setRetention();
            //getRetention();
            //setDispatchRate();
            //getDispatchRate();
            //setSubscriptionDispatchRate();
            //getSubscriptionDispatchRate();
            //setReplicatorDispatchRate();
            //getReplicatorDispatchRate();
            //getDeduplicationSnapshotInterval();
            //setDeduplicationSnapshotInterval();
            //removeDeduplicationSnapshotInterval();
            //unload();
            //setEntryFilters();
            //getEntryFilters();
            //removeEntryFilters();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            admin.close();
        }
    }

    private static void removeEntryFilters() throws PulsarAdminException {
        //删除条目过滤
        //您可以删除给定命名空间的条目过滤器策略。
        //pulsar-admin namespaces remove-entry-filters test-tenant/namespace1
        //admin.namespaces().removeEntryFilters(namespace);
        admin.namespaces().removeNamespaceEntryFilters(NAMESPACE);
    }

    private static void getEntryFilters() throws PulsarAdminException {
        //获取条目过滤
        //您可以获得给定命名空间的已配置条目过滤器。
        //pulsar-admin namespaces get-entry-filters test-tenant/namespace1
        //admin.namespaces().getEntryFilters(namespace);
        EntryFilters namespaceEntryFilters = admin.namespaces().getNamespaceEntryFilters(NAMESPACE);
        logger.info("{}", namespaceEntryFilters);
    }

    private static void setEntryFilters() throws PulsarAdminException {
        //设置条目过滤
        //条目过滤器有助于在服务器端过滤消息。
        //pulsar-admin namespaces set-entry-filters \
        //    --desc "The description of the entry filter to be used for user help." \
        //    --entry-filters-name "The class name for the entry filter." \
        //    --entry-filters-dir "The directory for all the entry filter implementations." \
        //    test-tenant/namespace1
        /*admin.namespaces().setEntryFilters(NAMESPACE,
                new EntryFilters("desc", "classes name", "class files localtion"));*/
        admin.namespaces().setNamespaceEntryFilters(NAMESPACE, new EntryFilters());
    }

    private static void unload() throws PulsarAdminException {
        //使用unload该命令的子命令namespaces
        //pulsar-admin namespaces unload my-tenant/my-ns
        admin.namespaces().unload(NAMESPACE);
    }

    private static void removeDeduplicationSnapshotInterval() throws PulsarAdminException {
        //删除重复数据删除快照
        //删除deduplicationSnapshotInterval某个命名空间的配置（该命名空间下的每个主题都会按照这个时间间隔进行去重快照）。
        //pulsar-admin namespaces remove-deduplication-snapshot-interval test-tenant/namespace1
        admin.namespaces().removeDeduplicationSnapshotInterval(NAMESPACE);
    }

    private static void setDeduplicationSnapshotInterval() throws PulsarAdminException {
        //设置重复数据删除快照
        //deduplicationSnapshotInterval设置命名空间的配置。
        // 命名空间下的每个主题都会按照这个时间间隔进行去重快照。
        // brokerDeduplicationEnabled必须设置true为才能使该属性生效。
        // pulsar-admin namespaces set-deduplication-snapshot-interval test-tenant/namespace1 --interval 1000
        admin.namespaces().setDeduplicationSnapshotInterval(NAMESPACE, 1000);
    }

    private static void getDeduplicationSnapshotInterval() throws PulsarAdminException {
        //获取重复数据删除快照
        //显示deduplicationSnapshotInterval某个命名空间的配置（该命名空间下的每个主题都会按照这个时间间隔进行去重快照）
        //pulsar-admin namespaces get-deduplication-snapshot-interval test-tenant/namespace1
        Integer deduplicationSnapshotInterval = admin.namespaces().getDeduplicationSnapshotInterval(NAMESPACE);
        logger.info("{}", deduplicationSnapshotInterval);
    }

    private static void getReplicatorDispatchRate() throws PulsarAdminException {
        //获取
        //它显示了命名空间的配置消息速率（该命名空间下的主题每秒可以调度这么多消息）
        //pulsar-admin namespaces get-replicator-dispatch-rate test-tenant/namespace1
        DispatchRate replicatorDispatchRate = admin.namespaces().getReplicatorDispatchRate(NAMESPACE);
        logger.info("{}", replicatorDispatchRate);
    }

    private static void setReplicatorDispatchRate() throws PulsarAdminException {
        //设置
        //它设置给定命名空间下复制集群之间所有复制器的消息调度率。
        // 调度速率可以受到每 X 秒的消息数 ( msg-dispatch-rate) 或每 X 秒的消息字节数 ( byte-dispatch-rate) 的限制。
        // 调度速率以秒为单位，可通过 进行配置dispatch-rate-period。
        // msg-dispatch-rate和的默认值为byte-dispatch-rate-1，这会禁用限制。
        //pulsar-admin namespaces set-replicator-dispatch-rate test-tenant/namespace1 \
        //    --msg-dispatch-rate 1000 \
        //    --byte-dispatch-rate 1048576 \
        //    --dispatch-rate-period 1
        admin.namespaces().setReplicatorDispatchRate(NAMESPACE,
                new DispatchRateImpl(1000, 1048576, true, 1));
    }

    private static void getSubscriptionDispatchRate() throws PulsarAdminException {
        //获取
        //它显示了命名空间的配置消息速率（该命名空间下的主题每秒可以调度这么多消息）。
        //pulsar-admin namespaces get-subscription-dispatch-rate test-tenant/namespace1
        DispatchRate subscriptionDispatchRate = admin.namespaces().getSubscriptionDispatchRate(NAMESPACE);
        logger.info("{}-{}-{}", subscriptionDispatchRate.getDispatchThrottlingRateInByte(),
                subscriptionDispatchRate.getDispatchThrottlingRateInMsg(),
                subscriptionDispatchRate.getRatePeriodInSecond());
    }

    private static void setSubscriptionDispatchRate() throws PulsarAdminException {
        //设置
        //它设置给定命名空间下所有主题订阅的消息调度率。
        // 调度速率可以受到每 X 秒的消息数 ( msg-dispatch-rate) 或每 X 秒的消息字节数 ( byte-dispatch-rate) 的限制。
        // 调度速率以秒为单位，可通过 进行配置dispatch-rate-period。
        // msg-dispatch-rate和的默认值为byte-dispatch-rate-1，这会禁用限制。
        //pulsar-admin namespaces set-subscription-dispatch-rate test-tenant/namespace1 \
        //    --msg-dispatch-rate 1000 \
        //    --byte-dispatch-rate 1048576 \
        //    --dispatch-rate-period 1
        admin.namespaces().setSubscriptionDispatchRate(NAMESPACE,
                new DispatchRateImpl(1000, 1048576, true, 1));
    }

    private static void getDispatchRate() throws PulsarAdminException {
        //获取
        //它显示了命名空间的配置消息速率（该命名空间下的主题每秒可以调度这么多消息）
        //pulsar-admin namespaces get-dispatch-rate test-tenant/namespace1
        DispatchRate dispatchRate = admin.namespaces().getDispatchRate(NAMESPACE);
        logger.info("{}-{}", dispatchRate.getDispatchThrottlingRateInByte(), dispatchRate.getDispatchThrottlingRateInMsg());
    }

    private static void setDispatchRate() throws PulsarAdminException {
        //设置
        // 它设置给定命名空间下所有主题的消息调度率。
        // 调度速率可以受到每 X 秒的消息数 ( msg-dispatch-rate) 或每 X 秒的消息字节数 ( byte-dispatch-rate) 的限制。
        // 调度速率以秒为单位，可通过 进行配置dispatch-rate-period。
        // msg-dispatch-rate和的默认值为byte-dispatch-rate-1，这会禁用限制。
        // 如果 或 均未clusterDispatchRate配置topicDispatchRate，则调度限制将被禁用。
        // 如果topicDispatchRate没有配置，clusterDispatchRate则生效。
        // 如果topicDispatchRate配置则topicDispatchRate生效。

        //pulsar-admin namespaces set-dispatch-rate test-tenant/namespace1 \
        //    --msg-dispatch-rate 1000 \
        //    --byte-dispatch-rate 1048576 \
        //    --dispatch-rate-period 1
        admin.namespaces().setDispatchRate(NAMESPACE,
                new DispatchRateImpl(1000, 1048576, true, 1));
    }

    private static void getRetention() throws PulsarAdminException {
        //获得
        //它显示给定名称空间的保留信息。
        //pulsar-admin namespaces get-retention test-tenant/namespace1
        RetentionPolicies retention = admin.namespaces().getRetention(NAMESPACE);
        if (Objects.isNull(retention)) {
            return;
        }
        logger.info("{}, {}", retention.getRetentionTimeInMinutes(), retention.getRetentionSizeInMB());
    }

    private static void setRetention() throws PulsarAdminException {
        //设置
        //每个命名空间包含多个主题，每个主题的保留大小（存储大小）不应超过特定阈值或应存储一定时间。
        // 此命令有助于配置给定命名空间中主题的保留大小和时间。
        //pulsar-admin namespaces set-retention --size 100M --time 10m test-tenant/namespace1
        admin.namespaces().setRetention(NAMESPACE,
                new RetentionPolicies(10, 100));
    }

    private static void clearNamespaceBundleBacklogForSubscription() throws PulsarAdminException {
        //清除捆绑包
        //它清除属于特定 NamespaceBundle 的所有主题的所有消息积压。您还可以清除特定订阅的积压工作。
        //pulsar-admin namespaces clear-backlog \
        //    --bundle 0x00000000_0xffffffff \
        //    --sub my-subscription \
        //    test-tenant/namespace1
        admin.namespaces().clearNamespaceBundleBacklogForSubscription(NAMESPACE,
                "0x00000000_0xffffffff", "my-subscription");
    }

    private static void clearNamespaceBacklogForSubscription() throws PulsarAdminException {
        //清除命名
        //它清除属于特定命名空间的所有主题的所有消息积压。您还可以清除特定订阅的积压工作。
        //pulsar-admin namespaces clear-backlog --sub my-subscription test-tenant/namespace1
        admin.namespaces().clearNamespaceBacklogForSubscription(NAMESPACE, "my-subscription");
    }

    private static void removeNamespaceMessageTTL() throws PulsarAdminException {
        //删除消息
        //删除已配置命名空间的消息 TTL。
        //pulsar-admin namespaces remove-message-ttl test-tenant/namespace1
        admin.namespaces().removeNamespaceMessageTTL(NAMESPACE);
    }

    private static void getNamespaceMessageTTL() throws PulsarAdminException {
        //获取消息
        //设置命名空间的 message-ttl 后，您可以使用以下命令获取配置的值。此示例继续命令的示例set message-ttl，因此返回值为 100(s)。
        //pulsar-admin namespaces get-message-ttl test-tenant/namespace1
        Integer namespaceMessageTTL = admin.namespaces().getNamespaceMessageTTL(NAMESPACE);
        logger.info("namespace message TTL: {}", namespaceMessageTTL);
    }

    private static void setNamespaceMessageTTL() throws PulsarAdminException {
        //设置消息
        //您可以配置消息的生存时间（以秒为单位）。在下面的示例中，message-ttl 设置为 100 秒。
        //pulsar-admin namespaces set-message-ttl --messageTTL 100 test-tenant/namespace1
        admin.namespaces().setNamespaceMessageTTL(NAMESPACE, 100);
    }

    private static void splitNamespaceBundle() throws PulsarAdminException {
        //分割命名空间
        //一个命名空间包可以包含多个主题，但只能由一个代理提供服务。如果单个捆绑包在代理上造成过多负载，管理员可以使用以下命令拆分捆绑包，允许卸载一个或多个新捆绑包，从而平衡代理之间的负载
        //pulsar-admin namespaces split-bundle --bundle 0x00000000_0xffffffff test-tenant/namespace1
        //splitAlgorithmName: range_equally_divide, topic_count_equally_divide, specified_positions_divide, flow_or_qps_equally_divide
        admin.namespaces().splitNamespaceBundle(NAMESPACE,
                "0x00000000_0xffffffff", true, "flow_or_qps_equally_divide");
    }

    private static void unloadNamespaceBundle() throws PulsarAdminException {
        //卸载命名空间
        //命名空间捆绑是属于同一命名空间的虚拟主题组。如果代理因捆绑包数量而过载，此命令可以帮助从该代理卸载捆绑包，以便它可以由其他一些负载较少的代理提供服务。命名空间捆绑 ID 范围为 0x00000000 到 0xffffffff。
        //pulsar-admin namespaces unload --bundle 0x00000000_0xffffffff test-tenant/namespace1
        //pulsar-admin namespaces unload --bundle 0x00000000_0xffffffff test-tenant/namespace1
        //pulsar-admin namespaces unload --bundle 0x00000000_0xffffffff --destinationBroker broker1.use.org.com:8080 test-tenant/namespace1
        admin.namespaces().unloadNamespaceBundle(NAMESPACE, "0x00000000_0xffffffff");

    }

    private static void getPersistence() throws PulsarAdminException {
        // 可以获取给定命名空间的已配置持久性策略
        //pulsar-admin namespaces get-persistence test-tenant/namespace1
        PersistencePolicies persistence = admin.namespaces().getPersistence(NAMESPACE);
        logger.info("{}", persistence);
    }

    private static void setPersistence() throws PulsarAdminException {
        //设置持久性
        //持久化策略允许用户为给定命名空间下的所有主题消息配置持久化级别。
        //
        //Bookkeeper-ack-quorum：等待每个条目的ack（保证副本）数量，默认：2
        //
        //Bookkeeper-ensemble：用于主题的 bookies 数量，默认值：2
        //
        //Bookkeeper-write-quorum：每个条目要写入多少次，默认值：2
        //
        //ml-mark-delete-max-rate：标记删除操作的限制速率（0表示不限制），默认：0

        //pulsar-admin namespaces set-persistence \
        //    --bookkeeper-ack-quorum 2 --bookkeeper-ensemble 3 \
        //    --bookkeeper-write-quorum 2 --ml-mark-delete-max-rate 0 \
        //    test-tenant/namespace1

        admin.namespaces().
                setPersistence(NAMESPACE,
                        new PersistencePolicies(2,
                                2,
                                2,
                                0));
    }

    private static void removeBacklogQuota() throws PulsarAdminException {
        //删除积压配额
        //您可以删除给定命名空间的积压配额策略。
        //pulsar-admin namespaces remove-backlog-quota test-tenant/namespace1
        admin.namespaces().removeBacklogQuota(NAMESPACE, BacklogQuota.BacklogQuotaType.destination_storage);
    }

    private static void getBacklogQuotaMap() throws PulsarAdminException {
        //获取积压配额
        //可以获得给定命名空间的已配置待办事项配额
        //pulsar-admin namespaces get-backlog-quotas test-tenant/namespace1
        Map<BacklogQuota.BacklogQuotaType, BacklogQuota> backlogQuotaMap = admin.namespaces().getBacklogQuotaMap(NAMESPACE);
        backlogQuotaMap.forEach((k, v) -> logger.info("k: {}, v: {}", k, v));
    }

    private static void setBacklogQuota() throws PulsarAdminException {
        //设置积压配额
        //积压配额可帮助代理在命名空间达到特定阈值限制时限制其带宽/存储。管理员可以设置限制，并在达到限制后采取相应的操作。
        //
        //Producer_request_hold：生产者保留消息并重试，直到sendTimeoutMs超出客户端配置
        //
        //Producer_Exception：生产者在尝试发送消息时抛出异常
        //
        //Consumer_backlog_eviction：代理开始丢弃积压消息
        //
        //可以通过定义 backlog-quota-type：destination_storage 的限制来处理积压配额限制。
        //
        //pulsar-admin namespaces set-backlog-quota --limit 10G \
        //    --limitTime 36000 \
        //    --policy producer_request_hold \
        //    test-tenant/namespace1

        admin.namespaces().setBacklogQuota(NAMESPACE, new BacklogQuotaImpl(100 * 1024 * 1024, 36000, BacklogQuota.RetentionPolicy.producer_request_hold));
    }

    private static void getNamespaceReplicationClusters() throws PulsarAdminException {
        //获得给定命名空间的复制集群列表
        //pulsar-admin namespaces get-clusters test-tenant/cluster1/namespace1
        List<String> namespaceReplicationClusters = admin.namespaces().getNamespaceReplicationClusters(NAMESPACE);
        namespaceReplicationClusters.forEach(logger::info);
    }

    private static void setNamespaceReplicationClusters() throws PulsarAdminException {
        //可以为命名空间设置复制集群，以使 Pulsar 能够在内部将已发布的消息从一个托管设施复制到另一个托管设施
        // pulsar-admin namespaces set-clusters test-tenant/namespace1 --clusters cl1
        admin.namespaces().setNamespaceReplicationClusters(NAMESPACE, Set.of("cluster-a"));
    }

    private static void deleteNamespace() throws PulsarAdminException {
        //pulsar-admin namespaces delete test-tenant/namespace1
        admin.namespaces().deleteNamespace(NAMESPACE);
    }

    private static void getNamespaces() throws PulsarAdminException {
        //pulsar-admin namespaces list test-tenant
        List<String> namespaces = admin.namespaces().getNamespaces(TENANT);
        namespaces.forEach(logger::info);
    }

    private static void getPolicies() throws PulsarAdminException {
        //pulsar-admin namespaces policies test-tenant/test-namespace
        Policies policies = admin.namespaces().getPolicies(NAMESPACE);
        logger.info("{}", policies);
    }

    private static void createNamespace() throws PulsarAdminException {
        //pulsar-admin namespaces create test-tenant/test-namespace
        admin.namespaces().createNamespace(NAMESPACE);
    }

}
