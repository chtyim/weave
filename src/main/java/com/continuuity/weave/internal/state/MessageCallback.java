package com.continuuity.weave.internal.state;

/**
 *
 */
public interface MessageCallback {

  void onReceived(String messageId, byte[] message);
}
