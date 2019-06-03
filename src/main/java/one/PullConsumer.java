package one;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
* @Description:    拉取型消费者
* @Author:         ll
* @CreateDate:     2019/5/31 16:40
* @UpdateDate:     2019/5/31 16:40
*/
public class PullConsumer {

    private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<>();

    public static void main(String[] args) {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("consumerGroup");
        consumer.setInstanceName(UUID.randomUUID().toString());
        consumer.setMessageModel(MessageModel.BROADCASTING);
        consumer.setNamesrvAddr("127.0.0.1:9876");

        try {
            consumer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        try {
            Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("qch_20190531");
            for (MessageQueue mq : mqs) {
                System.out.println("consume from the queue:" + mq + "%n");
                SINGLE_MQ:
                while (true) {
                    try {
                        PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
                        System.out.printf("%s%n", pullResult);
                        putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                        switch (pullResult.getPullStatus()) {
                            case FOUND:
                                break;
                            case NO_MATCHED_MSG:
                                break;
                            case NO_NEW_MSG:
                                break;
                            case OFFSET_ILLEGAL:
                                break;
                            default:
                                break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    private static void putMessageQueueOffset(MessageQueue mq, long nextBeginOffset) {
        OFFSE_TABLE.put(mq, nextBeginOffset);
    }

    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSE_TABLE.get(mq);
        if (offset != null)
            return offset;
        return 0;
    }
}
