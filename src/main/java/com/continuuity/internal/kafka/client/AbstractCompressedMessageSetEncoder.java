package com.continuuity.internal.kafka.client;

import com.google.common.base.Throwables;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;

import java.io.IOException;
import java.io.OutputStream;

/**
 *
 */
abstract class AbstractCompressedMessageSetEncoder extends AbstractMessageSetEncoder {

  private final Compression compression;
  private ChannelBufferOutputStream os;
  private OutputStream compressedOutput;


  protected AbstractCompressedMessageSetEncoder(Compression compression) {
    this.compression = compression;
    try {
      this.os = new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer());
      this.compressedOutput = createCompressedStream(os);
    } catch (IOException e) {
      // Should never happen
      throw Throwables.propagate(e);
    }
  }

  @Override
  public final MessageSetEncoder add(ChannelBuffer payload) {
    try {
      ChannelBuffer encoded = encodePayload(payload);
      encoded.readBytes(compressedOutput, encoded.readableBytes());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return this;

  }

  @Override
  public final ChannelBuffer finish() {
    try {
      compressedOutput.close();
      ChannelBuffer buf = prefixLength(encodePayload(os.buffer(), compression));
      compressedOutput = createCompressedStream(os);
      os.buffer().clear();

      return buf;

    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

  }

  protected abstract OutputStream createCompressedStream(OutputStream os) throws IOException;
}
