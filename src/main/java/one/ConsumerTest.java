package one;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.UUID;

/**
* @Description:    推送型消费者：监听消息进行消费
* @Author:         ll
* @CreateDate:     2019/5/31 16:39
* @UpdateDate:     2019/5/31 16:39
*/
public class ConsumerTest {
    public static void main(String[] args) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("con_qch_test");
        consumer.setInstanceName(UUID.randomUUID().toString());
        consumer.setConsumeMessageBatchMaxSize(32);
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt me : list) {
                    System.out.println(new String(me.getBody()));
                }
                System.out.println("==========");
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        try {
            consumer.subscribe("qch_20190531", "*");
            consumer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }
}
