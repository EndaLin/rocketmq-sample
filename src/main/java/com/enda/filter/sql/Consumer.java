package com.enda.filter.sql;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 负载均衡模式消费消息，多个消费者共同消费队列消息
 * @author linwt
 * @date 2020/4/27 10:34
 */
public class Consumer {
    public static void main(String[] args) throws MQClientException {
        //1、创建Consumer.指定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("LoadBalancingConsumer");
        // 2、指定NameServer 的地址
        consumer.setNamesrvAddr("39.106.204.246:9876");
        // 3、订阅Topic 和Tag
        consumer.subscribe("SQLTopic", MessageSelector.bySql("key>5"));

        // 4、创建消息监听器，多线程处理消息 MessageListenerConcurrently
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            // 接收消息内容
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msg, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for(MessageExt messageExt: msg) {
                    System.out.println(new String(messageExt.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 5、启动消费者
        consumer.start();
    }
}
