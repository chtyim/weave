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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 *
 */
final class KafkaResponseHandler extends SimpleChannelHandler {

  private final Bufferer bufferer = new Bufferer();

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    Object msg = e.getMessage();
    if (!(msg instanceof ChannelBuffer)) {
      super.messageReceived(ctx, e);
      return;
    }

    bufferer.apply((ChannelBuffer)msg);
    ChannelBuffer buffer = bufferer.getNext();
    while (buffer.readable()) {
      // Send the response object upstream
      Channels.fireMessageReceived(ctx, new KafkaResponse(KafkaResponse.ErrorCode.fromCode(buffer.readShort()),
                                                          buffer, buffer.readableBytes() + 6));
      buffer = bufferer.getNext();
    }
  }
}
