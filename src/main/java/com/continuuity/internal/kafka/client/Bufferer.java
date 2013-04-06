package com.continuuity.internal.kafka.client;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * A class to help buffering data of format [len][payload-of-len].
 */
final class Bufferer {

  private ChannelBuffer currentBuffer = null;
  private int currentSize = -1;

  void apply(ChannelBuffer buffer) {
    currentBuffer = concatBuffer(currentBuffer, buffer);
  }

  /**
   * Returns the buffer if the buffer data is ready to be consumed, otherwise return {@link ChannelBuffers#EMPTY_BUFFER}
   * @return
   */
  ChannelBuffer getNext() {
    if (currentSize < 0) {
      if (currentBuffer.readableBytes() < 4) {
        return ChannelBuffers.EMPTY_BUFFER;
      }
      currentSize = currentBuffer.readInt();
    }

    // Keep buffering if less then required number of bytes
    if (currentBuffer.readableBytes() < currentSize) {
      return ChannelBuffers.EMPTY_BUFFER;
    }

    ChannelBuffer result = currentBuffer.readSlice(currentSize);
    currentSize = -1;

    return result;
  }

  private ChannelBuffer concatBuffer(ChannelBuffer current, ChannelBuffer buffer) {
    return current == null ? buffer : ChannelBuffers.wrappedBuffer(current, buffer);
  }
}
