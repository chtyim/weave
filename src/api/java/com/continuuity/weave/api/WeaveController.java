package com.continuuity.weave.api;

import com.continuuity.weave.api.logging.LogHandler;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public interface WeaveController {

  RunInfo getRunInfo();

  void addLogHandler(LogHandler handler);

  ListenableFuture<?> stop();

  ListenableFuture<?> sendCommand(Command command);

  boolean waitFor(long timeout, TimeUnit timeoutUnit) throws InterruptedException;
}
