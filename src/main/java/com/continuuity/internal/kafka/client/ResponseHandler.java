package com.continuuity.internal.kafka.client;

/**
 *
 */
interface ResponseHandler {

  ResponseHandler NO_OP = new ResponseHandler() {
    @Override
    public void received(KafkaResponse response) {
      // No-op
    }
  };

  void received(KafkaResponse response);
}
