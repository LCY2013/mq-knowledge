package org.fufeng.knowledge.rocketmq.transaction;

import java.util.Random;

public class Order {

    //演示demo，模拟订单表查询服务，用来确认订单事务是否提交成功。
    public static boolean checkOrderById(String orderId) {
        Random random = new Random();
        boolean success = random.nextInt(2) < 1;
        System.out.println("checkOrderById: " + orderId + " status: " + success);
        return success;
    }

    //演示demo，模拟本地事务的执行结果。
    public static boolean doLocalTransaction() {
        Random random = new Random();
        boolean success = random.nextInt(2) < 1;
        System.out.println("doLocalTransaction status: " + success);
        return success;
    }

}
