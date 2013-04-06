package com.continuuity.weave.internal.kafka;

import com.continuuity.kafka.client.FetchedMessage;

import java.nio.ByteBuffer;

/**
 *
 */
final class BasicFetchedMessage implements FetchedMessage {

  private final long offset;
  private final ByteBuffer buffer;

  BasicFetchedMessage(long offset, ByteBuffer buffer) {
    this.offset = offset;
    this.buffer = buffer;
  }

  @Override
  public long getOffset() {
    return offset;
  }

  @Override
  public ByteBuffer getBuffer() {
    return buffer;
  }
}
