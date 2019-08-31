package org.apache.rocketmq.test;

import org.apache.log4j.Logger;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.factory.ConsumerFactory;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.smoke.NormalMessageSendAndRecvIT;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

/**
 * @author yuheng.hyh
 * @date 2019-08-31
 * @since 1.0, 2019-08-31 13:11
 */
public class RealTest {

    private static   Logger            logger      = Logger.getLogger(NormalMessageSendAndRecvIT.class);
    private          RMQNormalConsumer consumer    = null;
    private          RMQNormalProducer producer    = null;
    private          String            topic       = null;
    private          String            nsAddr;
    protected static int               consumeTime = 2 * 60 * 1000;

    @Before
    public void setUp() {
        topic = "TestTopic1";
        nsAddr = "127.0.0.1:9876";
        logger.info(String.format("use topic: %s;", topic));
        producer = new RMQNormalProducer(nsAddr, topic, false);
        producer.setDebug();
        consumer = ConsumerFactory.getRMQNormalConsumer(nsAddr, "consumerGroup",
            topic, "*", new RMQNormalListener(), false);
    }

    @After
    public void tearDown() {
        producer.shutdown();
        consumer.shutdown();
    }

    @Test
    public void testSynSendMessage() {
        int msgSize = 10;
        producer.send(msgSize);
        Assert.assertEquals("Not all sent succeeded", msgSize, producer.getAllUndupMsgBody().size());
        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);
        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
    }

}
