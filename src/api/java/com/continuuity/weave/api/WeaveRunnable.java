package com.continuuity.weave.api;

/**
 *
 */
public interface WeaveRunnable extends Runnable {

  WeaveRunnableSpecification configure();

  void initialize(WeaveContext context);

  void handleCommand(Command command) throws Exception;

  void stop();
}
