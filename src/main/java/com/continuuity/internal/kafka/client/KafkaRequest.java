package com.continuuity.internal.kafka.client;

import org.jboss.netty.buffer.ChannelBuffer;

/**
 *
 */
final class KafkaRequest {

  public enum Type {
    PRODUCE(0),
    FETCH(1),
    MULTI_FETCH(2),
    MULTI_PRODUCE(3),
    OFFSETS(4);

    private final short id;

    private Type(int id) {
      this.id = (short) id;
    }

    public short getId() {
      return id;
    }
  }

  private final Type type;
  private final String topic;
  private final int partition;
  private final ChannelBuffer body;
  private final ResponseHandler responseHandler;


  public static KafkaRequest createProduce(String topic, int partition, ChannelBuffer body) {
    return new KafkaRequest(Type.PRODUCE, topic, partition, body, ResponseHandler.NO_OP);
  }

  public static KafkaRequest createFetch(String topic, int partition, ChannelBuffer body, ResponseHandler handler) {
    return new KafkaRequest(Type.FETCH, topic, partition, body, handler);
  }

  private KafkaRequest(Type type, String topic, int partition, ChannelBuffer body, ResponseHandler responseHandler) {
    this.type = type;
    this.topic = topic;
    this.partition = partition;
    this.body = body;
    this.responseHandler = responseHandler;
  }

  Type getType() {
    return type;
  }

  String getTopic() {
    return topic;
  }

  int getPartition() {
    return partition;
  }

  ChannelBuffer getBody() {
    return body;
  }

  ResponseHandler getResponseHandler() {
    return responseHandler;
  }
}
