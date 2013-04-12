package com.continuuity.weave.internal.state;

import com.google.common.util.concurrent.ListenableFuture;

/**
 *
 */
public interface MessageCallback {

  /**
   * Called when a message is received.
   * @param message
   * @return A {@link ListenableFuture} that would be completed when message processing is completed or failed.
   *         The result of the future should be the input message Id if succeeded.
   */
  ListenableFuture<String> onReceived(Message message);
}
