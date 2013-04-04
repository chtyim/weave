package com.continuuity.weave.internal.kafka;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 *
 */
final class KafkaResponseDispatcher extends SimpleChannelHandler {
  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    Object attachment = ctx.getAttachment();
    if (e.getMessage() instanceof KafkaResponse && attachment instanceof ResponseHandler) {
      ((ResponseHandler) attachment).received((KafkaResponse) e.getMessage());
    } else {
      super.messageReceived(ctx, e);
    }
  }

  @Override
  public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    if (e.getMessage() instanceof KafkaRequest) {
      ctx.setAttachment(((KafkaRequest) e.getMessage()).getResponseHandler());
    }
    super.writeRequested(ctx, e);
  }
}
