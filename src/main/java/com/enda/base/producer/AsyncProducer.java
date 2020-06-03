package com.enda.base.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

/**
 * 发送异步消息
 * @author linwt
 * @date 2020/4/26 18:01
 */
public class AsyncProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        // 1. 创建消息生产者，并指定生产者名
        DefaultMQProducer producer = new DefaultMQProducer("async-mq-group");
        // 2. 指定NameServer 的地址
        producer.setNamesrvAddr("39.106.204.246:9876");
        // 3. 启动producer
        producer.start();

        for (int i = 0; i < 10; i++) {
            /**
             * 4. 创建消息对象，指定Topic、消息Tag和消息内容
             */
            Message msg = new Message("baseMsg", "async-baseTag", ("Mq test:" + i).getBytes());
            // 5. 发送异步消息，同步发送会阻塞等待MQ的回调
            producer.send(msg, new SendCallback() {
                /**
                 * 发送成功的回调函数
                 * @param sendResult /
                 */
                public void onSuccess(SendResult sendResult) {
                    System.out.println("发送结果：" + sendResult);
                }

                /**
                 * 发送失败的回调函数
                 * @param throwable /
                 */
                public void onException(Throwable throwable) {
                    System.out.println("发送异常：" + throwable.getMessage());
                }
            });

            TimeUnit.SECONDS.sleep(1);
        }

        // 6.关闭生产者
        producer.shutdown();
    }
}
