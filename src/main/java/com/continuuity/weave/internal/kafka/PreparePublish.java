package com.continuuity.weave.internal.kafka;

import com.google.common.util.concurrent.ListenableFuture;

import java.nio.ByteBuffer;

/**
 *
 */
public interface PreparePublish {

  PreparePublish add(byte[] payload, Object partitionKey);

  PreparePublish add(ByteBuffer payload, Object partitionKey);

  ListenableFuture<?> publish();
}
