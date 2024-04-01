package org.fufeng.knowledge.pulsar.tableview;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.fufeng.knowledge.pulsar.common.Common.Client;

public class OpTableView {

    private static final Logger logger = LoggerFactory.getLogger(OpTableView.class);

    public static void main(String[] args) throws Exception {
        PulsarClient client = Client();
        TableView<String> tv = client.newTableViewBuilder(Schema.STRING)
                .topic("my-tableview")
                .create();

        // Register listeners for all existing and incoming messages
        tv.forEachAndListen((key, value) -> logger.info(key, value)/*operations on all existing and incoming messages*/);

        // Register actions for all existing messages
        tv.forEach((key, value) -> logger.info(key, value)/*operations on all existing messages*/);
    }

}
