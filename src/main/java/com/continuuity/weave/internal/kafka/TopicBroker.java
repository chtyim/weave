package com.continuuity.weave.internal.kafka;

import java.net.InetSocketAddress;

/**
 *
 */
final class TopicBroker {

  private final String topic;
  private final InetSocketAddress address;
  private final int partitionSize;

  TopicBroker(String topic, InetSocketAddress address, int partitionSize) {
    this.topic = topic;
    this.address = address;
    this.partitionSize = partitionSize;
  }

  String getTopic() {
    return topic;
  }

  InetSocketAddress getAddress() {
    return address;
  }

  int getPartitionSize() {
    return partitionSize;
  }
}
