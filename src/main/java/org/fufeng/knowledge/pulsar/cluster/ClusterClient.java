package org.fufeng.knowledge.pulsar.cluster;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.AutoClusterFailover;
import org.apache.pulsar.client.impl.ControlledClusterFailover;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ClusterClient {

    private PulsarClient getAutoFailoverClient() throws PulsarClientException {
        //构建 Java Pulsar 客户端以使用自动集群级故障转移
        String primaryUrl = "pulsar+ssl://localhost:6651";
        String secondaryUrl = "pulsar+ssl://localhost:6661";
        String primaryTlsTrustCertsFilePath = "primary/path";
        String secondaryTlsTrustCertsFilePath = "secondary/path";
        Authentication primaryAuthentication = AuthenticationFactory.create(
                "org.apache.pulsar.client.impl.auth.AuthenticationTls",
                "tlsCertFile:/path/to/primary-my-role.cert.pem,"
                        + "tlsKeyFile:/path/to/primary-role.key-pk8.pem");
        Authentication secondaryAuthentication = AuthenticationFactory.create(
                "org.apache.pulsar.client.impl.auth.AuthenticationTls",
                "tlsCertFile:/path/to/secondary-my-role.cert.pem,"
                        + "tlsKeyFile:/path/to/secondary-role.key-pk8.pem");

        // You can put more failover cluster config in to map
        Map<String, String> secondaryTlsTrustCertsFilePaths = new HashMap<>();
        secondaryTlsTrustCertsFilePaths.put(secondaryUrl, secondaryTlsTrustCertsFilePath);
        Map<String, Authentication> secondaryAuthentications = new HashMap<>();
        secondaryAuthentications.put(secondaryUrl, secondaryAuthentication);
        ServiceUrlProvider failover = AutoClusterFailover.builder()
                .primary(primaryUrl)
                .secondary(List.of(secondaryUrl))
                .failoverDelay(30, TimeUnit.SECONDS)
                .switchBackDelay(60, TimeUnit.SECONDS)
                .checkInterval(1000, TimeUnit.MILLISECONDS)
                .secondaryTlsTrustCertsFilePath(secondaryTlsTrustCertsFilePaths)
                .secondaryAuthentication(secondaryAuthentications)
                .build();

        PulsarClient pulsarClient = PulsarClient.builder()
                .authentication(primaryAuthentication)
                .tlsTrustCertsFilePath(primaryTlsTrustCertsFilePath)
                .build();

        failover.initialize(pulsarClient);
        return pulsarClient;
    }

    public PulsarClient getControlledFailoverClient() throws IOException {
        //构建 Java Pulsar 客户端以使用受控集群级故障转移
        Map<String, String> header = new HashMap();
        header.put("service_user_id", "my-user");
        header.put("service_password", "tiger");
        header.put("clusterA", "tokenA");
        header.put("clusterB", "tokenB");

        ServiceUrlProvider provider =
                ControlledClusterFailover.builder()
                        .defaultServiceUrl("pulsar://localhost:6650")
                        .checkInterval(1, TimeUnit.MINUTES)
                        .urlProvider("http://localhost:8080/test")
                        .urlProviderHeader(header)
                        .build();

        PulsarClient pulsarClient =
                PulsarClient.builder()
                        .build();

        provider.initialize(pulsarClient);
        return pulsarClient;
    }
}
