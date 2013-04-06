package com.continuuity.weave.internal.kafka;

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
