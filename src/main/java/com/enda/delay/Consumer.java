package com.enda.delay;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * @author linwt
 * @date 2020/5/19 16:44
 */
public class Consumer {
    public static void main(String[] args) throws MQClientException {
        //1、创建Consumer.指定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("LoadBalancingConsumer");
        // 2、指定NameServer 的地址
        consumer.setNamesrvAddr("39.106.204.246:9876");
        // 3、订阅Topic 和Tag
        consumer.subscribe("DelayTopic", "baseTag");

        // 设置消费者模式：负载均衡(默认)| 广播模式
        consumer.setMessageModel(MessageModel.CLUSTERING);

        // 4、创建消息监听器，多线程处理消息 MessageListenerConcurrently
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            // 接收消息内容
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msg, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for(MessageExt messageExt: msg) {
                    System.out.println("接收消息，延迟：" + (System.currentTimeMillis() - messageExt.getStoreTimestamp()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 5、启动消费者
        consumer.start();
    }
}
