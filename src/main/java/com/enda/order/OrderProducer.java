package com.enda.order;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

/**
 * 顺序发送
 *
 * @author linwt
 * @date 2020/5/19 11:03
 */
public class OrderProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        // 1. 创建消息生产者，并指定生产者名
        DefaultMQProducer producer = new DefaultMQProducer("async-mq-group");
        // 2. 指定NameServer 的地址
        producer.setNamesrvAddr("39.106.204.246:9876");
        // 3. 启动producer
        producer.start();

        // 获取订单
        List<OrderStep> orderSteps = OrderUtils.buildOrders();

        // 发送消息
        for (OrderStep step : orderSteps) {
            Message msg = new Message("OrderTopic", "order", (step.getOrderId() + ":" + step.getDesc()).getBytes());
            /**
             * 参数一：消息对象
             * 参数二：消息队列的选择器
             * 参数三：选择队列的业务标识（订单ID）
             */
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                /**
                 * 保证同一个订单的一系列操作都在同一个队列中，以保证顺序消费
                 *
                 * @param list 队列的集合
                 * @param message 消息对象
                 * @param args 业务标识参数
                 * @return 选中的队列
                 */
                public MessageQueue select(List<MessageQueue> list, Message message, Object args) {
                    Long orderId = (Long) args;
                    long index = orderId % list.size();
                    return list.get((int) index);
                }
            }, step.getOrderId());

            System.out.println("发送结果：" + sendResult);
        }

        producer.shutdown();
    }
}
