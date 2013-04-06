package com.continuuity.kafka.client;

import java.nio.ByteBuffer;

/**
 *
 */
public interface FetchedMessage {

  long getOffset();

  ByteBuffer getBuffer();
}
