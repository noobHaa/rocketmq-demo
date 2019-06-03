package one;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.UUID;

public class ProducerTest {
    private static DefaultMQProducer producer = null;

    public static void main(String[] args) {
        System.out.println("[======]start\n");
        int pro_count = 1;
        if (args.length > 0) {
            pro_count = Integer.parseInt(args[0]);
        }
        boolean result = false;
        try {
            ProducerStart();
            for (int i = 0; i < pro_count; i++) {
                String msg = "hello rocketmq" + i + "====" + UUID.randomUUID().toString();
                SendMessage("qch_20190531", msg);
                System.out.println(msg);
            }
        } finally {
            producer.shutdown();
        }
        System.out.println("[======]successed");
    }

    private static boolean SendMessage(String topic, String str) {
        Message message = new Message(topic, str.getBytes());
        try {
            producer.send(message);
        } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private static boolean ProducerStart() {
        producer = new DefaultMQProducer("pro_qch_test");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setInstanceName(UUID.randomUUID().toString());
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
