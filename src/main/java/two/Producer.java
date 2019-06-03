package two;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class Producer {
    public static void main(String[] args) throws MQClientException {
        /**
         * 一个应用创建一个producer，由应用来维护次对象，可以设置全局对象或者单例
         * 注意：producerGroupName需要由应用来保证唯一
         * producerGroup这个概念发送普通消息时，作用不大，但是发送分布式事务消息是，比较关键
         * 因为服务器会回查这个group下的任意一个producer
         * */
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
        /**
         * producer对象在使用之前需要调用start初始化，初始化一次即可
         * 注意：不可在每次发送消息时都使用start方法
         * */
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        try {
            /**
             * 下面这段代码表明一个producer对象可以发送多个topic
             * 注意：send方法是同步调用，只要不抛异常就标识成功。但是发送成功也可能有多重状态
             * 例如消息写入master成功但是slave不成功，
             * */
            {
                Message msg = new Message("TopicTest1",
                        "TagA",
                        "OrderID001",
                        ("hello metaQ").getBytes());

                SendResult sendResult = producer.send(msg);
                System.out.println(sendResult);
            }
            {
                Message msg = new Message("TopicTest2",// topic
                        "TagB",// tag
                        "OrderID0034",// key
                        ("Hello MetaQ").getBytes());// body
                SendResult sendResult = producer.send(msg);
                System.out.println(sendResult);
            }

            {
                Message msg = new Message("TopicTest3",// topic
                        "TagC",// tag
                        "OrderID061",// key
                        ("Hello MetaQ").getBytes());// body
                SendResult sendResult = producer.send(msg);
                System.out.println(sendResult);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        /**
         * 应用退出时，要调用shutdown来清理资源，关闭网络连接，从metaQ服务器上注销自己
         * 注意：建议在jBoss、tomcat等容器的退出钩子里面调用shutdown方法
         * */
        producer.shutdown();
    }
}
