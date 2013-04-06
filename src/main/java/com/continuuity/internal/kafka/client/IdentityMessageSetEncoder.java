package com.continuuity.internal.kafka.client;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 *
 */
final class IdentityMessageSetEncoder extends AbstractMessageSetEncoder {

  private ChannelBuffer messageSets = ChannelBuffers.EMPTY_BUFFER;

  @Override
  public MessageSetEncoder add(ChannelBuffer payload) {
    messageSets = ChannelBuffers.wrappedBuffer(messageSets, encodePayload(payload));
    return this;
  }

  @Override
  public ChannelBuffer finish() {
    ChannelBuffer buf = prefixLength(messageSets);
    messageSets = ChannelBuffers.EMPTY_BUFFER;
    return buf;
  }
}
