package com.continuuity.weave.internal.kafka;

import java.nio.ByteBuffer;

/**
 *
 */
public final class FetchedMessage {

  private final long offset;
  private final ByteBuffer buffer;

  FetchedMessage(long offset, ByteBuffer buffer) {
    this.offset = offset;
    this.buffer = buffer;
  }

  public long getOffset() {
    return offset;
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }
}
