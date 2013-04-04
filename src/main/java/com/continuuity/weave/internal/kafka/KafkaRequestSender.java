package com.continuuity.weave.internal.kafka;

/**
 *
 */
interface KafkaRequestSender {

  void send(KafkaRequest request);
}
