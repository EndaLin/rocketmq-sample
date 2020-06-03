package com.enda.base.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

/**
 * 单向发送消息，没有关心发送结果
 *
 * @author linwt
 * @date 2020/4/27 10:26
 */
public class OneWayProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        // 1. 创建消息生产者，并指定生产者名
        DefaultMQProducer producer = new DefaultMQProducer("one-way-mq-group");
        // 2. 指定NameServer 的地址
        producer.setNamesrvAddr("39.106.204.246:9876");
        // 3. 启动producer
        producer.start();

        for (int i = 0; i < 3; i++) {
            /**
             * 4. 创建消息对象，指定Topic、消息Tag和消息内容
             */
            Message msg = new Message("one-way", "baseTag", ("Mq test:" + i).getBytes());
            // 5. 发送单向消息
            producer.sendOneway(msg);

            TimeUnit.SECONDS.sleep(1);
        }

        // 6.关闭生产者
        producer.shutdown();
    }
}
