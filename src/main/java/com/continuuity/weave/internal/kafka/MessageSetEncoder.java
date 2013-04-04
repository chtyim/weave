package com.continuuity.weave.internal.kafka;

import org.jboss.netty.buffer.ChannelBuffer;

/**
 *
 */
interface MessageSetEncoder {

  MessageSetEncoder add(ChannelBuffer payload);

  ChannelBuffer finish();
}
