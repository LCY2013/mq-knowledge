package org.fufeng.knowledge.rocketmq.transaction;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.apis.producer.Transaction;
import org.apache.rocketmq.client.apis.producer.TransactionResolution;
import org.apache.rocketmq.client.java.message.MessageBuilderImpl;
import org.apache.rocketmq.shaded.com.google.common.base.Strings;
import org.fufeng.knowledge.rocketmq.normal.NormalPushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import static org.fufeng.knowledge.rocketmq.common.Common.getTxProducer;

/**
 * 创建延时队列
 * ./bin/mqadmin updatetopic -n localhost:9876 -t TransactionTopic -c DefaultCluster -a +message.type=TRANSACTION
 */

public class TransactionProducer {
    private static final Logger logger = LoggerFactory.getLogger(NormalPushConsumer.class);

    public static void main(String[] args) throws ClientException, IOException {
        Producer producer = getTxProducer((messageView) -> {
            /*
             * 事务检查器一般是根据业务的ID去检查本地事务是否正确提交还是回滚，此处以订单ID属性为例。
             * 在订单表找到了这个订单，说明本地事务插入订单的操作已经正确提交；如果订单表没有订单，说明本地事务已经回滚。
             */
            final String orderId = messageView.getProperties().get("OrderId");
            if (Strings.isNullOrEmpty(orderId)) {
                // 错误的消息，直接返回Rollback。
                return TransactionResolution.ROLLBACK;
            }
            return Order.checkOrderById(orderId) ? TransactionResolution.COMMIT : TransactionResolution.ROLLBACK;
        });
        //开启事务分支。
        final Transaction transaction;
        try {
            transaction = producer.beginTransaction();
        } catch (ClientException e) {
            logger.error("beginTransaction err:", e);
            //事务分支开启失败，直接退出。
            return;
        }

        //事务消息发送。
        MessageBuilder messageBuilder = new MessageBuilderImpl();
        String orderId = UUID.randomUUID().toString();
        Message message = messageBuilder.setTopic("TransactionTopic")
                //设置消息索引键，可根据关键字精确查找某条消息。
                .setKeys(UUID.randomUUID().toString())
                //设置消息Tag，用于消费端根据指定Tag过滤消息。
                .setTag("TransactionTopic")
                //一般事务消息都会设置一个本地事务关联的唯一ID，用来做本地事务回查的校验。
                .addProperty("OrderId", orderId)
                //消息体。
                .setBody("messageBody".getBytes())
                .build();
        //发送半事务消息
        final SendReceipt sendReceipt;
        try {
            sendReceipt = producer.send(message, transaction);
        } catch (ClientException e) {
            //半事务消息发送失败，事务可以直接退出并回滚。
            return;
        }
        logger.info("sendReceipt message: {}, orderId: {}", sendReceipt.getMessageId(), orderId);

        /*
         * 执行本地事务，并确定本地事务结果。
         * 1. 如果本地事务提交成功，则提交消息事务。
         * 2. 如果本地事务提交失败，则回滚消息事务。
         * 3. 如果本地事务未知异常，则不处理，等待事务消息回查。
         *
         */
        Random random = new Random();
        boolean success = random.nextInt(2) < 1;
        if (success) {
            boolean localTransactionOk = Order.doLocalTransaction();
            if (localTransactionOk) {
                try {
                    transaction.commit();
                } catch (ClientException e) {
                    // 业务可以自身对实时性的要求选择是否重试，如果放弃重试，可以依赖事务消息回查机制进行事务状态的提交。
                    logger.error("transaction.commit() err:", e);
                }
            } else {
                try {
                    transaction.rollback();
                } catch (ClientException e) {
                    // 建议记录异常信息，回滚异常时可以无需重试，依赖事务消息回查机制进行事务状态的提交。
                    logger.error("transaction.rollback() err:", e);
                }
            }
        }

        //producer.close();
    }

}
