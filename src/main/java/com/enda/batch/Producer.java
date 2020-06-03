package com.enda.batch;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;

/**
 * @author linwt
 * @date 2020/5/19 17:07
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        // 1. 创建消息生产者，并指定生产者名
        DefaultMQProducer producer = new DefaultMQProducer("mq-group");
        // 2. 指定NameServer 的地址
        producer.setNamesrvAddr("39.106.204.246:9876");
        // 3. 启动producer
        producer.start();

        /**
         * 4. 创建消息对象，指定Topic、消息Tag和消息内容
         * 批量发送，每次发送需要少于4M
         */
        List<Message> msgs = new ArrayList<Message>(3);
        msgs.add(new Message("batch", "tag", "message_1".getBytes()));
        msgs.add(new Message("batch", "tag", "message_2".getBytes()));
        msgs.add(new Message("batch", "tag", "message_3".getBytes()));

        // 5. 发送消息，同步发送会阻塞等待MQ的回调
        SendResult result = producer.send(msgs);
        // 6. 分析回调结果
        System.out.println(String.format("结果：%s", result));

        // 6.关闭生产者
        producer.shutdown();
    }
}
