package com.enda.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * @author linwt
 * @date 2020/5/19 12:47
 */
public class OrderConsumer {
    public static void main(String[] args) throws MQClientException {
        //1、创建Consumer.指定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("LoadBalancingConsumer");
        // 2、指定NameServer 的地址
        consumer.setNamesrvAddr("39.106.204.246:9876");
        // 3、订阅Topic 和Tag
        consumer.subscribe("OrderTopic", "order");

        // 4、注册消息监听器，一个队列只对应一个线程
        consumer.registerMessageListener(new MessageListenerOrderly() {
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg: msgs) {
                    System.out.println("线程名称：【" + Thread.currentThread().getName() + "】" + new String(msg.getBody()));
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();
        System.out.println("消费者启动");
    }
}
