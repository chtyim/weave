package com.continuuity.weave;

import com.continuuity.weave.internal.kafka.Compression;
import com.continuuity.weave.internal.kafka.FetchedMessage;
import com.continuuity.weave.internal.kafka.KafkaClient;
import com.continuuity.weave.internal.kafka.PreparePublish;
import com.continuuity.weave.internal.kafka.SimpleKafkaClient;
import com.continuuity.weave.zk.InMemoryZKServer;
import com.google.common.base.Charsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class KafkaTest {


  @Test
  public void test() throws InterruptedException {
    KafkaClient kafkaClient = new SimpleKafkaClient("localhost:2181");
    kafkaClient.startAndWait();

    TimeUnit.SECONDS.sleep(50);

    kafkaClient.stopAndWait();
  }

  @Test
  public void testProduce() throws InterruptedException, ExecutionException {
    KafkaClient kafkaClient = new SimpleKafkaClient("localhost:2181");
    kafkaClient.startAndWait();

    PreparePublish preparePublish = kafkaClient.preparePublish("testtopic", Compression.GZIP);
//    PreparePublish preparePublish = kafkaClient.preparePublish("testtopic", Compression.NONE);
    for (int i = 0; i < 20; i += 10) {
      for (int j = i ; j < i + 10; j++) {
        preparePublish.add((j + " Testing message").getBytes(Charsets.UTF_8), j);
      }
      preparePublish.publish().get();
      System.out.println("Published: " + i);
      TimeUnit.MILLISECONDS.sleep(100);
    }

    kafkaClient.stopAndWait();
  }

  @Test
  public void testConsume() throws InterruptedException {
    KafkaClient kafkaClient = new SimpleKafkaClient("localhost:2181");
    kafkaClient.startAndWait();

    Iterator<FetchedMessage> consumer = kafkaClient.consume("testtopic", 0, 0, 300);
    while (consumer.hasNext()) {
      FetchedMessage message = consumer.next();
      System.out.println(message.getOffset() + " " + Charsets.UTF_8.decode(message.getBuffer()));
    }

    TimeUnit.SECONDS.sleep(2);

    kafkaClient.stopAndWait();
  }
}
