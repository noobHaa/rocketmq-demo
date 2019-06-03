package one;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;
import java.util.UUID;

public class PushConsumerTest {
    private static int count=0;

    public static void main(String[] args) {
        System.out.println("Push consumer main start");
        count=0;
        DefaultMQPushConsumer consumer=new DefaultMQPushConsumer("con_group_1");
        consumer.setInstanceName(UUID.randomUUID().toString());
        consumer.setMessageModel(MessageModel.BROADCASTING);
        consumer.setConsumeMessageBatchMaxSize(32);
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println("list count="+list.size());
                for (MessageExt me:list){
                    count++;
                    System.out.println("count="+count+",msg="+new String(me.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        try {
            consumer.subscribe("qch_20190532","*");
            consumer.start();
            System.out.println("Push consumer started");
        } catch (MQClientException e) {
            e.printStackTrace();
        }

    }
}
