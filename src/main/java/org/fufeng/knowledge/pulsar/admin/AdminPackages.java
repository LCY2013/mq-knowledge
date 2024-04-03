package org.fufeng.knowledge.pulsar.admin;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.fufeng.knowledge.pulsar.common.Common.Admin;

/**
 * 包是用户希望在以后的操作中重用的一组元素。在 Pulsar 中，一个包可以是一组函数、源和接收器。您可以根据您的需要定义一个包。
 *
 * Pulsar 中的包管理系统存储每个包的数据和元数据（如下表所示）并跟踪包版本。
 *
 * 元数据	            描述
 * 描述	            包的描述。
 * 接触	            包裹的联系信息。例如，开发团队的电子邮件地址。
 * 创建时间	        创建包的时间。
 * 修改时间	        包最后被修改的时间。
 * 特性	            用户定义的键/值映射来存储其他信息。
 *
 * 通过提供以下信息在包管理器中创建包：类型、租户、命名空间、包名称和版本。
 *
 * 成分	                描述
 * 类型	            指定受支持的包类型之一：函数、接收器和源。
 * 租户	            指定要在其中创建包的租户。
 * 名称空间	        指定要在其中创建包的命名空间。
 * 姓名	            使用格式指定包的完整名称<tenant>/<namespace>/<package name>。
 * 版本	            使用数字格式指定包的版本MajorVerion.MinorVersion。
 *
 * 您提供的信息会创建包的 URL，格式为<type>://<tenant>/<namespace>/<package name>/<version>。
 *
 * 将元素上传到包，即您想要跨命名空间使用的函数、源和接收器。
 *
 * 从不同的命名空间向该包应用权限。
 *
 * 可以通过从包管理器中调用此包来使用您在包中定义的元素。包管理器通过 URL 找到它。例如，
 * sink://public/default/mysql-sink@1.0
 * function://my-tenant/my-ns/my-function@0.1
 * source://my-tenant/my-ns/mysql-cdc-source@2.3
 *
 * 要使用包管理服务，请确保您的集群中已启用包管理服务，方法是在broker.conf
 * enablePackagesManagement=true
 * packagesManagementStorageProvider=org.apache.pulsar.packages.management.storage.bookkeeper.BookKeeperPackagesStorageProvider
 * packagesReplicas=1
 * packagesManagementLedgerRootPath=/ledgers
 */
public class AdminPackages {

    private static final Logger logger = LoggerFactory.getLogger(AdminPackages.class);

    private static final PulsarAdmin admin = Admin();

    private static final String TENANT = "public";

    private static final String NAMESPACE = "public/test-namespace";

    public static void main(String[] args) {
        try {
            //upload();
            //download();
            //delete();
            //getMetadata();
            //updateMetadata();
            //listPackageVersions();
            listPackages();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            admin.close();
        }
    }


    private static void listPackages() throws PulsarAdminException {
        //列出
        //您可以使用以下命令列出命名空间下特定类型的所有包。
        //bin/pulsar-admin packages list --type function public/default
        List<String> packages = admin.packages().listPackages("function", NAMESPACE);
        packages.forEach(logger::info);
    }

    private static void listPackageVersions() throws PulsarAdminException {
        //列出
        //您可以使用以下命令列出软件包的所有版本。
        //bin/pulsar-admin packages list-versions type://tenant/namespace/packageName
        List<String> versions = admin.packages().listPackageVersions("functions://public/default/example@v0.1");
        versions.forEach(logger::info);
    }

    private static void updateMetadata() throws PulsarAdminException {
        //更新
        //您可以使用以下命令来更新包的元数据。
        //bin/pulsar-admin packages update-metadata function://public/default/example@v0.1 --description update-description
        admin.packages().updateMetadata("functions://public/default/example@v0.1",
                new PackageMetadata().toBuilder().description("update desc").build());
    }

    private static void getMetadata() throws PulsarAdminException {
        //获取
        //您可以使用以下命令来获取包的元数据。
        //bin/pulsar-admin packages get-metadata function://public/default/test@v1
        PackageMetadata metadata = admin.packages().getMetadata("functions://public/default/example@v0.1");
        logger.info("{}", metadata);
    }

    private static void delete() throws PulsarAdminException {
        //删除
        //您可以使用以下命令来删除包。
        //bin/pulsar-admin packages delete functions://public/default/example@v0.1
        admin.packages().delete("functions://public/default/example@v0.1");
    }

    private static void download() throws PulsarAdminException {
        //下载一个
        //您可以使用以下命令来下载软件包。
        //bin/pulsar-admin packages download function://public/default/example@v0.1 --path package-file
        admin.packages().download("function://public/default/example@v0.1", "./");
    }

    private static void upload() throws PulsarAdminException {
        //上传
        //您可以使用以下命令上传包。
        //bin/pulsar-admin packages upload function://public/default/example@v0.1 --path package-file --description package-description
        admin.packages().upload(
                new PackageMetadata().toBuilder().
                        createTime(System.currentTimeMillis()).
                        contact("fufeng").description("demo")
                .build(), "function://public/default/example@v0.1", "./");
    }

}
