package org.fufeng.knowledge.pulsar.admin;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.List;

import static org.fufeng.knowledge.pulsar.common.Common.*;

public class AdminClusters {

    private static final Logger logger = LoggerFactory.getLogger(AdminClusters.class);

    private static final PulsarAdmin admin = Admin();

    public static final String ClusterName = "admin-api-cluster";

    public static void main(String[] args) throws Exception {
        //createCluster();
        //getCluster();
        //updateCluster();
        //updatePeerClusterNames();
        //getClusters();
        deleteCluster();

        admin.close();
    }

    private static void deleteCluster() throws PulsarAdminException {
        //pulsar-admin clusters delete admin-api-cluster
        admin.clusters().deleteCluster(ClusterName);
    }

    private static void getClusters() throws PulsarAdminException {
        //pulsar-admin clusters list
        List<String> clusters = admin.clusters().getClusters();
        clusters.forEach(logger::info);
    }

    private static void updatePeerClusterNames() throws PulsarAdminException {
        //可以为 Pulsar实例中的给定集群配置对等集群
        //pulsar-admin update-peer-clusters cluster-1 --peer-clusters cluster-2
        LinkedHashSet<String> clusterSet = new LinkedHashSet<>();
        clusterSet.add(ClusterName);
        admin.clusters().updatePeerClusterNames(ClusterName, clusterSet);
    }

    private static void updateCluster() throws PulsarAdminException {
        //pulsar-admin clusters update admin-api-cluster \
        //    --url http://my-cluster.org.com:4081 \
        //    --broker-url pulsar://my-cluster.org.com:3350
        ClusterData clusterData = ClusterDataImpl.builder().
                brokerServiceUrl(serviceUrl).
                serviceUrl(serviceRestUrl).
                build();
        admin.clusters().updateCluster(ClusterName, clusterData);
    }

    private static void getCluster() throws PulsarAdminException {
        //pulsar-admin clusters get admin-api-cluster
        ClusterData cluster = admin.clusters().getCluster(ClusterName);
        logger.info("{} {}", cluster.getServiceUrl(), cluster.getBrokerServiceUrl());
    }

    private static void createCluster() throws PulsarAdminException {
        //pulsar-admin clusters create admin-api-cluster \
        //    --url http://my-cluster.org.com:8080 \
        //    --broker-url pulsar://my-cluster.org.com:6650
        ClusterData clusterData = ClusterDataImpl.builder().
                brokerServiceUrl(serviceUrl).
                serviceUrl(serviceRestUrl).
                build();
        admin.clusters().createCluster(ClusterName, clusterData);
    }

}
