package one;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PushConsumerThreadPollTest {
    public static void main(String[] args) {
        int threadCount = 3;
        int waitTime = 60000;

        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            Runnable runner = new ExecutorThread(String.valueOf(i));
        }
    }
}

class ExecutorThread implements Runnable {
    private String name = "";
    private int count = 0;

    ExecutorThread(String name) {
        this.name = name;
    }

    @Override
    public void run() {
        StartPushConsumer();
    }

    private void StartPushConsumer() {
        System.out.println("consumer name=" + name);
        count = 0;
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("con_group_1");
        consumer.setInstanceName(UUID.randomUUID().toString());
        //广播消费
//        consumer.setMessageModel(MessageModel.BROADCASTING);
        //集群消费
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeMessageBatchMaxSize(32);
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.registerMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> {
            System.out.println("list count=" + list.size());
            for (MessageExt me : list) {
                count++;
                System.out.println("name=" + name + ",count=" + count + "msg=" + new String(me.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        try {
            consumer.subscribe("qch_20190531", "*");
            consumer.start();
            System.out.println("consumer started.name=" + name);
        } catch (MQClientException e) {
            e.printStackTrace();
        }

    }
}
