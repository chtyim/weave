package com.continuuity.weave;

import com.continuuity.weave.internal.kafka.Compression;
import com.continuuity.weave.internal.kafka.FetchedMessage;
import com.continuuity.weave.internal.kafka.KafkaClient;
import com.continuuity.weave.internal.kafka.PreparePublish;
import com.continuuity.weave.internal.kafka.SimpleKafkaClient;
import com.google.common.base.Charsets;
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

    PreparePublish preparePublish = kafkaClient.preparePublish(topic, Compression.GZIP);
    for (int i = 0 ; i < 10; i++) {
      preparePublish.add((i + " GZip Testing message").getBytes(Charsets.UTF_8), 0);
    }
    preparePublish.publish().get();

    preparePublish = kafkaClient.preparePublish(topic, Compression.NONE);
    for (int i = 0 ; i < 10; i++) {
      preparePublish.add((i + " Testing message").getBytes(Charsets.UTF_8), 0);
    }
    preparePublish.publish().get();

    Iterator<FetchedMessage> consumer = kafkaClient.consume(topic, 0, 0, 1048576);
    int count = 0;
    long startTime = System.nanoTime();
    while (count < 20 && consumer.hasNext() && secondsPassed(startTime, TimeUnit.NANOSECONDS) < 5) {
      System.out.println(Charsets.UTF_8.decode(consumer.next().getBuffer()));
      count++;
    }

    Assert.assertEquals(20, count);

    kafkaClient.stopAndWait();
  }

  private long secondsPassed(long startTime, TimeUnit startUnit) {
    return TimeUnit.SECONDS.convert(System.nanoTime() - TimeUnit.NANOSECONDS.convert(startTime, startUnit),
                                    TimeUnit.NANOSECONDS);
  }
}
