package org.fufeng.knowledge.pulsar.common;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class Common {

    public static final String ClusterName = "cluster-a";

    public static final String serviceUrl = "pulsar://localhost:6650";
    public static final String serviceRestUrl = "http://localhost:8080";

    public static PulsarClient Client() {
        try {
            return PulsarClient.builder()
                    .serviceUrl(serviceUrl)
                    .build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    public static PulsarAdmin Admin() {
        try {
            String url = "http://localhost:8080";
            PulsarAdmin admin = PulsarAdmin.builder()
                    .serviceHttpUrl(url)
                    .build();
            return admin;
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
}
