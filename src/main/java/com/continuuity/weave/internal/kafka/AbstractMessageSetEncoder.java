package com.continuuity.weave.internal.kafka;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.util.zip.CRC32;

/**
 *
 */
abstract class AbstractMessageSetEncoder implements MessageSetEncoder {

  private static final ThreadLocal<CRC32> CRC32_LOCAL = new ThreadLocal<CRC32>() {
    @Override
    protected CRC32 initialValue() {
      return new CRC32();
    }
  };

  protected final int computeCRC32(ChannelBuffer buffer) {
    CRC32 crc32 = CRC32_LOCAL.get();
    crc32.reset();

    if (buffer.hasArray()) {
      crc32.update(buffer.array(), buffer.arrayOffset() + buffer.readerIndex(), buffer.readableBytes());
    } else {
      byte[] bytes = new byte[buffer.readableBytes()];
      buffer.getBytes(buffer.readerIndex(), bytes);
      crc32.update(bytes);
    }
    return (int) crc32.getValue();
  }

  protected final ChannelBuffer encodePayload(ChannelBuffer payload) {
    return encodePayload(payload, Compression.NONE);
  }

  protected final ChannelBuffer encodePayload(ChannelBuffer payload, Compression compression) {
    ChannelBuffer header = ChannelBuffers.buffer(10);

    int crc = computeCRC32(payload);

    int magic = ((compression == Compression.NONE) ? 0 : 1);

    // Message length = 1 byte magic + (optional 1 compression byte) + 4 bytes crc + payload length
    header.writeInt(5 + magic + payload.readableBytes());
    // Magic number = 0 for non-compressed data
    header.writeByte(magic);
    if (magic > 0) {
      header.writeByte(compression.getCode());
    }
    header.writeInt(crc);

    return ChannelBuffers.wrappedBuffer(header, payload);
  }

  protected final ChannelBuffer prefixLength(ChannelBuffer buffer) {
    ChannelBuffer sizeBuf = ChannelBuffers.buffer(4);
    sizeBuf.writeInt(buffer.readableBytes());
    return ChannelBuffers.wrappedBuffer(sizeBuf, buffer);
  }
}
