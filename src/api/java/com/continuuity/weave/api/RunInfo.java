package com.continuuity.weave.api;

/**
 *
 */
public interface RunInfo {

  /**
   * Returns an unique id representing the running {@link WeaveRunnable}.
   * @return An unique id
   */
  RunId getId();

  /**
   * Returns the {@link WeaveRunnableSpecification} returned from
   * the {@link com.continuuity.weave.api.WeaveRunnable#configure()} method.
   * @return A {@link WeaveRunnableSpecification} instance.
   */
  WeaveRunnableSpecification getSpecification();

  ResourceSpecification getResourceSpecification();
}
