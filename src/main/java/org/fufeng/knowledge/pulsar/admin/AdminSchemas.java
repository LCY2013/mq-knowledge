package org.fufeng.knowledge.pulsar.admin;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.fufeng.knowledge.pulsar.common.Common.Admin;

public class AdminSchemas {

    private static final Logger logger = LoggerFactory.getLogger(AdminSchemas.class);

    private static final PulsarAdmin admin = Admin();

    private static final String TENANT = "public";

    private static final String NAMESPACE = "public/test-namespace";

    public static void main(String[] args) {
        try {
            //createSchema();
            //getSchema();
            //getSchemaByVersion();
            //deleteSchema();
            //setIsAllowAutoUpdateSchema();
            //setSchemaValidationEnforced();
            //setSchemaCompatibilityStrategy();
            //setSchemaCompatibilityStrategyNamespace();
            //getSchemaCompatibilityStrategy();
            //getSchemaCompatibilityStrategy();
            getSchemaCompatibilityStrategyNamespace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            admin.close();
        }
    }

    private static void getSchemaCompatibilityStrategyNamespace() throws PulsarAdminException {
        //获取命名空间级架构兼容性
        //您可以使用以下方法之一获取命名空间级别的架构兼容性检查策略。
        //pulsar-admin namespaces get-schema-compatibility-strategy options
        //admin.namespaces().getSchemaCompatibilityStrategy("test", SchemaCompatibilityStrategy.FULL);
    }

    private static void getSchemaCompatibilityStrategy() throws PulsarAdminException {
        //获取架构兼容性
        //获取主题级架构兼容性
        //要获取主题级架构兼容性检查策略，您可以使用以下方法之一。
        //pulsar-admin topicPolicies get-schema-compatibility-strategy <topicName>
        // get the current applied schema compatibility strategy
        admin.topicPolicies().getSchemaCompatibilityStrategy("my-tenant/my-ns/my-topic", true);

        // only get the schema compatibility strategy from topic policies
        admin.topicPolicies().getSchemaCompatibilityStrategy("my-tenant/my-ns/my-topic", false);
    }

    //设置集群级架构兼容性
    //要在集群级别设置架构兼容性检查策略，请在文件schemaCompatibilityStrategy中设置conf/broker.conf。
    //schemaCompatibilityStrategy=ALWAYS_INCOMPATIBLE

    private static void setSchemaCompatibilityStrategyNamespace() throws PulsarAdminException {
        //设置命名空间级架构兼容性
        //要在命名空间级别设置架构兼容性检查策略，可以使用以下方法之一。
        //pulsar-admin namespaces set-schema-compatibility-strategy options
        admin.namespaces().setSchemaCompatibilityStrategy("test", SchemaCompatibilityStrategy.FULL);
    }

    private static void setSchemaCompatibilityStrategy() throws PulsarAdminException {
        //管理架构兼容性
        //不同级别配置的模式兼容性检查策略的优先级为：主题级别 > 命名空间级别 > 集群级别。换句话说：
        //
        //如果在主题级别和命名空间级别都设置策略，则使用主题级别策略。
        //如果在命名空间和集群级别都设置了策略，则使用命名空间级别的策略。

        //设置架构兼容性
        //设置主题级架构兼容性
        //要在主题级别设置架构兼容性检查策略，您可以使用以下方法之一。
        //pulsar-admin topicPolicies set-schema-compatibility-strategy <strategy> <topicName>
        admin.topicPolicies().setSchemaCompatibilityStrategy("my-tenant/my-ns/my-topic", SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE);
    }

    private static void setSchemaValidationEnforced() throws PulsarAdminException {
        //启用架构验证
        //要在集群级别强制实施架构验证，您可以在文件中isSchemaValidationEnforced进行配置。trueconf/broker.conf
        //
        //要在命名空间级别启用架构验证强制实施，您可以使用以下命令之一。
        //bin/pulsar-admin namespaces set-schema-validation-enforce --enable tenant/namespace
        admin.namespaces().setSchemaValidationEnforced("my-namspace", true);

        //禁用模式验证
        //要在命名空间级别禁用架构验证强制实施，您可以使用以下命令之一。
        //bin/pulsar-admin namespaces set-schema-validation-enforce --disable tenant/namespace
        admin.namespaces().setSchemaValidationEnforced("my-namspace", false);
    }

    private static void setIsAllowAutoUpdateSchema() throws PulsarAdminException {
        //启用架构
        //要在命名空间级别启用/强制架构自动更新，您可以使用以下方法之一。
        //bin/pulsar-admin namespaces set-is-allow-auto-update-schema --enable tenant/namespace
        admin.namespaces().setIsAllowAutoUpdateSchema("my-namspace", true);
    }

    private static void deleteSchema() throws PulsarAdminException {
        //在任何情况下，该delete操作都会删除为主题注册的模式的所有版本。
        //pulsar-admin schemas delete <topic-name>
        admin.schemas().deleteSchema("my-tenant/my-ns/my-topic");
    }

    private static void extractSchema() throws PulsarAdminException {
        //提取
        //要通过主题提取（提供）模式，请使用以下方法
        //pulsar-admin schemas extract --classname <class-name> --jar <jar-path> --type <type-name>
    }
    private static void getSchemaByVersion() throws PulsarAdminException {
        //获取特定
        //要获取架构的特定版本，您可以使用以下方法之一。
        //pulsar-admin schemas get <topic-name> --version <version>
        SchemaInfo si = admin.schemas().getSchemaInfo("my-tenant/my-ns/my-topic", 1L);
        logger.info("{}", new String(si.getSchema()));
    }

    private static void getSchema() throws PulsarAdminException {
        //获取最新
        //要获取主题的最新架构，您可以使用以下方法之一。
        //pulsar-admin schemas get <topic-name>

        SchemaInfo si = admin.schemas().getSchemaInfo("my-tenant/my-ns/my-topic");
        logger.info("{}", new String(si.getSchema()));
    }

    /**
     * 如果架构是原始架构，则该schema字段必须为空。
     * 如果 schema 是struct schema，则该字段必须是 Avro schema 定义的 JSON 字符串。
     * <p>
     * 负载包括以下字段：
     * <p>
     * 场地	                    描述
     * type             下页列出了基元类型架构的允许值：基元类型
     *                  结构类型模式允许的值为AVRO、PROTOBUF、PROTOBUF_NATIVE和JSON。
     * <p>
     * schema	        架构定义数据，以 UTF 8 字符集编码。
     *                  如果架构类型是AVRO、PROTOBUF或JSON架构，则此字段应该是JSON 格式的Avro 架构定义。
     *                  如果模式类型是PROTOBUF_NATIVE模式，则此字段应包含 Protobuf 描述符。
     *                  否则，该字段应为空。
     * <p>
     * json约束示例
     * {
     *     "type": "JSON",
     *     "schema": "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"com.foo\",\"fields\":[{\"name\":\"file1\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"file2\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"file3\",\"type\":[\"string\",\"null\"],\"default\":\"dfdf\"}]}",
     *     "properties": {}
     * }
     * @throws PulsarAdminException exception
     */
    private static void createSchema() throws PulsarAdminException {
        //上传
        //要上传（注册）主题的新架构，您可以使用以下方法之一。
        //pulsar-admin schemas upload --filename <schema-definition-file> <topic-name>
        PostSchemaPayload payload = new PostSchemaPayload();
        payload.setType("INT8");
        payload.setSchema("");

        admin.schemas().createSchema("my-tenant/my-ns/my-topic", payload);
    }

}
