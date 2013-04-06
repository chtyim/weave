package com.continuuity.internal.kafka.client;

import org.jboss.netty.buffer.ChannelBuffer;

/**
 *
 */
interface MessageSetEncoder {

  MessageSetEncoder add(ChannelBuffer payload);

  ChannelBuffer finish();
}
