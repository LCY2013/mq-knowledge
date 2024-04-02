package org.fufeng.knowledge.pulsar.admin;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pulsar.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.fufeng.knowledge.pulsar.common.Common.Admin;

public class AdminFunctions {

    private static final Logger logger = LoggerFactory.getLogger(AdminFunctions.class);

    private static final PulsarAdmin admin = Admin();

    public static void main(String[] args) throws Exception {
        //getFunction();
        //getFunctions();
        //getFunctionStatus();
        //getSignalFunctionStatus();
        //createFunction();
        //updateFunction();
        //startFunction();
        //signalStartFunction();
        //stopFunction();
        //signalStopFunction();
        //restartFunction();
        //signalRestartFunction();
        //deleteFunction();
        //triggerFunction();
        //putFunctionState();
        getFunctionState();

        admin.close();
    }

    private static void getFunctionState() throws PulsarAdminException {
        //pulsar-admin functions querystate \
        //    --tenant public \
        //    --namespace default \
        //    --name (the name of Pulsar Functions) \
        //    --key (the key of state)
        FunctionState functionState = admin.functions().getFunctionState("public", "default", "ExclamationFunction", "pulsar");
        logger.info("{}", functionState.getKey());
    }

    private static void putFunctionState() throws PulsarAdminException, JsonProcessingException {
        //pulsar-admin functions putstate \
        //    --tenant public \
        //    --namespace default \
        //    --name (the name of Pulsar Functions) \
        //    --state "{\"key\":\"pulsar\", \"stringValue\":\"hello pulsar\"}"
        TypeReference<FunctionState> typeRef = new TypeReference<FunctionState>() {
        };
        FunctionState stateRepr = ObjectMapperFactory.getThreadLocal().readValue("{\"key\":\"pulsar\", \"stringValue\":\"hello pulsar\"}", typeRef);
        admin.functions().putFunctionState("public", "default", "ExclamationFunction", stateRepr);
    }

    private static void triggerFunction() throws PulsarAdminException {
        //pulsar-admin functions trigger \
        //    --tenant public \
        //    --namespace default \
        //    --name (the name of Pulsar Functions) \
        //    --topic (the name of input topic) \
        //    --trigger-value \"hello pulsar\"
        //    # or --trigger-file (the path of trigger file)
        admin.functions().triggerFunction("public", "default", "ExclamationFunction", "persistent://public/default/test-output-topic", "hello pulsar", AdminFunctions.class.getResource("./examples/api-examples.jar").getPath());
    }

    private static void getSignalFunctionStatus() throws PulsarAdminException {
        //pulsar-admin functions status \
        //    --tenant public \
        //    --namespace default \
        //    --name (the name of Pulsar Functions)
        FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionStatus = admin.functions().getFunctionStatus("public", "default", "ExclamationFunction", 0);
        logger.info("running: {}", functionStatus.running);
    }

    private static void getFunction() throws PulsarAdminException {
        //pulsar-admin functions get \
        //    --tenant public \
        //    --namespace default \
        //    --name (the name of Pulsar Functions)
        FunctionConfig function = admin.functions().getFunction("public", "default", "ExclamationFunction");
        logger.info("{} {} {}", function.getClassName(), function.getFunctionType(), function.getJar());
    }

    private static void deleteFunction() throws PulsarAdminException {
        //pulsar-admin functions delete \
        //    --tenant public \
        //    --namespace default \
        //    --name (the name of Pulsar Functions)
        admin.functions().deleteFunction("public", "default", "ExclamationFunction");
    }

    private static void signalRestartFunction() throws PulsarAdminException {
        //pulsar-admin functions restart \
        //    --tenant public \
        //    --namespace default \
        //    --name (the name of Pulsar Functions)
        admin.functions().restartFunction("public", "default", "ExclamationFunction");
    }

    private static void restartFunction() throws PulsarAdminException {
        //pulsar-admin functions restart \
        //    --tenant public \
        //    --namespace default \
        //    --name (the name of Pulsar Functions) \
        //    --instance-id 1
        admin.functions().restartFunction("public", "default", "ExclamationFunction", 0);
    }

    private static void signalStopFunction() throws PulsarAdminException {
        //pulsar-admin functions stop \
        //    --tenant public \
        //    --namespace default \
        //    --name (the name of Pulsar Functions)
        admin.functions().stopFunction("public", "default", "ExclamationFunction");
    }

    private static void stopFunction() throws PulsarAdminException {
        //pulsar-admin functions stop \
        //    --tenant public \
        //    --namespace default \
        //    --name (the name of Pulsar Functions) \
        //    --instance-id 1
        admin.functions().stopFunction("public", "default", "ExclamationFunction", 0);
    }

    private static void signalStartFunction() throws PulsarAdminException {
        //pulsar-admin functions start \
        //    --tenant public \
        //    --namespace default \
        //    --name (the name of Pulsar Functions) \
        admin.functions().startFunction("public", "default", "ExclamationFunction");
    }

    private static void startFunction() throws PulsarAdminException {
        //pulsar-admin functions start \
        //    --tenant public \
        //    --namespace default \
        //    --name (the name of Pulsar Functions) \
        //    --instance-id 1
        admin.functions().startFunction("public", "default", "ExclamationFunction", 0);
    }

    private static void updateFunction() throws PulsarAdminException {
        //pulsar-admin functions update \
        //    --tenant public \
        //    --namespace default \
        //    --name (the name of Pulsar Functions) \
        //    --output persistent://public/default/update-output-topic \
        //    # other options
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant("public");
        functionConfig.setNamespace("default");
        functionConfig.setName("ExclamationFunction");
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setParallelism(1);
        functionConfig.setClassName("org.apache.pulsar.functions.api.examples.ExclamationFunction");
        UpdateOptions updateOptions = new UpdateOptionsImpl();
        updateOptions.setUpdateAuthData(false);
        String path = AdminFunctions.class.getResource("./examples/api-examples.jar").getPath();
        admin.functions().updateFunction(functionConfig, path, updateOptions);
    }

    private static void createFunction() throws PulsarAdminException {
        //pulsar-admin functions create \
        //    --tenant public \
        //    --namespace default \
        //    --name (the name of Pulsar Functions) \
        //    --inputs test-input-topic \
        //    --output persistent://public/default/test-output-topic \
        //    --classname org.apache.pulsar.functions.api.examples.ExclamationFunction \
        //    --jar /examples/api-examples.jar
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant("public");
        functionConfig.setNamespace("default");
        functionConfig.setName("ExclamationFunction");
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setParallelism(1);
        functionConfig.setClassName("org.apache.pulsar.functions.api.examples.ExclamationFunction");
        functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        functionConfig.setTopicsPattern("test-input-topic");
        functionConfig.setSubName("exclamationFunction-sub-name");
        functionConfig.setAutoAck(true);
        functionConfig.setOutput("persistent://public/default/test-output-topic");
        String path = AdminFunctions.class.getResource("./examples/api-examples.jar").getPath();
        // fileName 本地文件
        admin.functions().createFunction(functionConfig, path);
    }

    private static void getFunctionStatus() throws PulsarAdminException {
        //pulsar-admin functions status \
        //    --tenant public \
        //    --namespace default \
        //    --name (the name of Pulsar Functions)
        FunctionStatus functionStatus = admin.functions().getFunctionStatus("public", "default", "ExclamationFunction");
        functionStatus.instances.forEach(functionInstanceStatus -> logger.info("instance Id: {}, status: {}", functionInstanceStatus.getInstanceId(), functionInstanceStatus.getStatus()));
    }

    private static void getFunctions() throws PulsarAdminException {
        //pulsar-admin functions list \
        //    --tenant public \
        //    --namespace default
        List<String> functions = admin.functions().getFunctions("public", "default");
        functions.forEach(logger::info);
    }
}
