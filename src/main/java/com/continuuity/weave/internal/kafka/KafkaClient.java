package com.continuuity.weave.internal.kafka;

import com.google.common.util.concurrent.Service;

import java.util.Iterator;

/**
 *
 */
public interface KafkaClient extends Service {

  PreparePublish preparePublish(String topic, Compression compression);

  Iterator<FetchedMessage> consume(String topic, int partition, long offset, int maxSize);
}
