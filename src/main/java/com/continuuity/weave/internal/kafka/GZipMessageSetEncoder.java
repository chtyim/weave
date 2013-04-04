package com.continuuity.weave.internal.kafka;

import com.google.common.base.Throwables;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;

import java.io.IOException;
import java.util.zip.GZIPOutputStream;

/**
 *
 */
final class GZipMessageSetEncoder extends AbstractMessageSetEncoder {

  private ChannelBuffer result;
  private GZIPOutputStream os;

  GZipMessageSetEncoder() {
    try {
      this.result = ChannelBuffers.dynamicBuffer();
      this.os = new GZIPOutputStream(new ChannelBufferOutputStream(result));
    } catch (IOException e) {
      // Should never happen
      throw Throwables.propagate(e);
    }
  }

  @Override
  public MessageSetEncoder add(ChannelBuffer payload) {
    // Write to GZIP
    try {
      ChannelBuffer encoded = encodePayload(payload);
      encoded.readBytes(os, encoded.readableBytes());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return this;
  }

  @Override
  public ChannelBuffer finish() {
    try {
      os.close();
      ChannelBuffer buf = prefixLength(encodePayload(result, Compression.GZIP));

      os = new GZIPOutputStream(new ChannelBufferOutputStream(result));
      result.clear();

      return buf;

    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
