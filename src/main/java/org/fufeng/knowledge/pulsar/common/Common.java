package org.fufeng.knowledge.pulsar.common;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class Common {

    public static final String serviceUrl = "pulsar://localhost:6650";

    public static PulsarClient Client() {
        try {
            return PulsarClient.builder()
                    .serviceUrl(serviceUrl)
                    .build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
}
