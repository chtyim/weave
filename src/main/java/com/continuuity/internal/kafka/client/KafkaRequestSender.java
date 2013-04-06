package com.continuuity.internal.kafka.client;

/**
 *
 */
interface KafkaRequestSender {

  void send(KafkaRequest request);
}
