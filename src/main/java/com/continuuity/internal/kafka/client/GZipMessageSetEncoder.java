package com.continuuity.internal.kafka.client;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 *
 */
final class GZipMessageSetEncoder extends AbstractCompressedMessageSetEncoder {

  GZipMessageSetEncoder() {
    super(Compression.GZIP);
  }

  @Override
  protected OutputStream createCompressedStream(OutputStream os) throws IOException {
    return new GZIPOutputStream(os);
  }
}
