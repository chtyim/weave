package com.continuuity.weave.internal.kafka;

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
