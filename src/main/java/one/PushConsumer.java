package one;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.UUID;

public class PushConsumer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        //生成consumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("con_group_1");
        //配置consumer
        consumer.setInstanceName(UUID.randomUUID().toString());
        consumer.setMessageModel(MessageModel.BROADCASTING);
        consumer.setConsumeMessageBatchMaxSize(32);
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.registerMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> {
            for (MessageExt me : list) {
                System.out.println("msg="+new String(me.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        //启动consumer
        consumer.subscribe("qch_20190531","*");
        consumer.start();

        //停止consumer
        Thread.sleep(60000);
        consumer.shutdown();
    }
}
