package com.continuuity.weave.internal.kafka;

/**
 *
 */
public interface ResponseHandler {

  ResponseHandler NO_OP = new ResponseHandler() {
    @Override
    public void received(KafkaResponse response) {
      // No-op
    }
  };

  void received(KafkaResponse response);
}
