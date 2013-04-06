package com.continuuity.weave;

import com.continuuity.kafka.client.FetchedMessage;
import com.continuuity.kafka.client.KafkaClient;
import com.continuuity.kafka.client.PreparePublish;
import com.continuuity.weave.internal.kafka.Compression;
import com.continuuity.weave.internal.kafka.SimpleKafkaClient;
import com.google.common.base.Charsets;
import com.google.common.util.concurrent.Futures;
import junit.framework.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class KafkaTest {

  @Test
  @Ignore
  public void testKafka() throws InterruptedException, ExecutionException {
    KafkaClient kafkaClient = new SimpleKafkaClient("localhost:2181");
    kafkaClient.startAndWait();

    String topic = "topic" + System.currentTimeMillis();

    Thread t1 = createPublishThread(kafkaClient, topic, Compression.GZIP, "GZIP Testing message", 10);
    Thread t2 = createPublishThread(kafkaClient, topic, Compression.NONE, "Testing message", 10);

    t1.start();
    t2.start();

    Thread t3 = createPublishThread(kafkaClient, topic, Compression.NONE, "More Testing message", 10);
    t2.join();
    t3.start();

    Iterator<FetchedMessage> consumer = kafkaClient.consume(topic, 0, 0, 1048576);
    int count = 0;
    long startTime = System.nanoTime();
    while (count < 30 && consumer.hasNext() && secondsPassed(startTime, TimeUnit.NANOSECONDS) < 5) {
      System.out.println(Charsets.UTF_8.decode(consumer.next().getBuffer()));
      count++;
    }

    Assert.assertEquals(30, count);

    kafkaClient.stopAndWait();
  }

  private Thread createPublishThread(final KafkaClient kafkaClient, final String topic,
                                     final Compression compression, final String message, final int count) {
    return new Thread() {
      public void run() {
        PreparePublish preparePublish = kafkaClient.preparePublish(topic, compression);
        for (int i = 0 ; i < count; i++) {
          preparePublish.add((i + " " + message).getBytes(Charsets.UTF_8), 0);
        }
        Futures.getUnchecked(preparePublish.publish());
      }
    };
  }

  private long secondsPassed(long startTime, TimeUnit startUnit) {
    return TimeUnit.SECONDS.convert(System.nanoTime() - TimeUnit.NANOSECONDS.convert(startTime, startUnit),
                                    TimeUnit.NANOSECONDS);
  }
}
