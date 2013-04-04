package com.continuuity.weave.api;

/**
 * The <code>WeaveRunnable</code> interface should be implemented by any
 * class whose instances are intended to be executed in a Weave cluster.
 */
public interface WeaveRunnable extends Runnable {

  /**
   * Called at the submission time. Executed on the client side.
   * @return A {@link WeaveRunnableSpecification} built by {@link WeaveRunnableSpecification.Builder}.
   */
  WeaveRunnableSpecification configure();

  /**
   * Called when the container process starts. Executed in container machine.
   * @param context Contains information about the runtime context.
   */
  void initialize(WeaveContext context);

  /**
   * Called when a command is received. A normal return denotes the command has been processed successfully, otherwise
   * {@link Exception} should be thrown.
   * @param command Contains details of the command.
   * @throws Exception
   */
  void handleCommand(Command command) throws Exception;

  /**
   * Requests to stop the running service.
   */
  void stop();
}
