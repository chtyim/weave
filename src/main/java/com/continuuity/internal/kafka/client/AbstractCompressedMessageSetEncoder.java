/**
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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
