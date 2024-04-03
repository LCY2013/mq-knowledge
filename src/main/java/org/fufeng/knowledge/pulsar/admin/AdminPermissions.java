package org.fufeng.knowledge.pulsar.admin;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import static org.fufeng.knowledge.pulsar.common.Common.Admin;

/**
 * Pulsar 允许您向用户授予命名空间级别或主题级别的权限。
 * <p>
 * 如果为用户授予命名空间级别的权限，则该用户可以访问该命名空间下的所有主题。
 * <p>
 * 如果您为用户授予主题级别的权限，则该用户只能访问该主题。
 */
public class AdminPermissions {

    private static final Logger logger = LoggerFactory.getLogger(AdminPermissions.class);

    private static final PulsarAdmin admin = Admin();

    private static final String TENANT = "public";

    private static final String NAMESPACE = "public/test-namespace";

    public static void main(String[] args) {
        try {
            //grantPermissionOnNamespace();
            //getPermissions();
            revokePermissionsOnNamespace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            admin.close();
        }
    }

    private static void revokePermissionsOnNamespace() throws PulsarAdminException {
        //撤销
        //您可以撤销特定角色的权限，这意味着这些角色将不再有权访问指定的命名空间。
        //pulsar-admin namespaces revoke-permission test-tenant/namespace1 \
        //      --role admin10
        admin.namespaces().revokePermissionsOnNamespace(NAMESPACE, "admin");
    }

    private static void getPermissions() throws PulsarAdminException {
        //获取
        //您可以查看命名空间中的哪些角色被授予了哪些权限。
        //pulsar-admin namespaces permissions test-tenant/namespace1
        Map<String, Set<AuthAction>> permissions = admin.namespaces().getPermissions(NAMESPACE);
        permissions.forEach((k, v) -> logger.info("k: {}, v: {}", k, v));
    }

    private static void grantPermissionOnNamespace() throws PulsarAdminException {
        //授予
        //您可以向特定角色授予操作列表的权限，例如produce和consume。
        //pulsar-admin namespaces grant-permission test-tenant/namespace1 \
        //    --actions produce,consume \
        //    --role admin10
        admin.namespaces().grantPermissionOnNamespace(NAMESPACE, "admin", Set.of(AuthAction.produce, AuthAction.consume));
    }

}
