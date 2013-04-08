package com.continuuity.internal.kafka.client;

import org.xerial.snappy.SnappyOutputStream;

import java.io.IOException;
import java.io.OutputStream;

/**
 *
 */
final class SnappyMessageSetEncoder extends AbstractCompressedMessageSetEncoder {

  SnappyMessageSetEncoder() {
    super(Compression.SNAPPY);
  }

  @Override
  protected OutputStream createCompressedStream(OutputStream os) throws IOException {
    return new SnappyOutputStream(os);
  }
}
