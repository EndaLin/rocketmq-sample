package com.enda.filter.sql;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

/**
 * 发送同步消息，发送消息时，阻塞等待MQ 的收到确认信息
 * 对数据发送的可靠性要求比较高
 *
 * @author linwt
 * @date 2020/4/26 17:26
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        // 1. 创建消息生产者，并指定生产者名
        DefaultMQProducer producer = new DefaultMQProducer("mq-group");
        // 2. 指定NameServer 的地址
        producer.setNamesrvAddr("39.106.204.246:9876");
        // 3. 启动producer
        producer.start();

        for (int i = 0; i < 10; i++) {
            /**
             * 4. 创建消息对象，指定Topic、消息Tag和消息内容
             */
            Message msg = new Message("SQLTopic", "baseTag", ("Mq test:" + i).getBytes());

            // 给消息额外添加属性，用于过滤用
            msg.putUserProperty("key", String.valueOf(i));
            // 5. 发送消息，同步发送会阻塞等待MQ的回调
            SendResult result = producer.send(msg);
            // 6. 分析回调结果
            System.out.println(String.format("结果：%s", result));

            TimeUnit.SECONDS.sleep(1);
        }

        // 6.关闭生产者
        producer.shutdown();
    }
}
