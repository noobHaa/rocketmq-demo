package one;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.UUID;

public class Producer {
    public static void main(String[] args) {
        //生成procuder
        DefaultMQProducer producer = new DefaultMQProducer("pro_qch_test");
        //配置producer
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setInstanceName(UUID.randomUUID().toString());
        //启动producer
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
            return;
        }

        //生产消息
        String str="Hello RocketMQ!====="+UUID.randomUUID().toString();
        Message message=new Message("qch_20190531",str.getBytes());

        try {
            producer.send(message);
        } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
            e.printStackTrace();
            return;
        }
        //停止producer
        producer.shutdown();
        System.out.println("[=======]success\n");
    }
}
