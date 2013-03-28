package com.continuuity.weave.api;

/**
 *
 */
public interface ErrorHandler {

  enum InstanceDiedPolicy {
    RELAUNCH,
    IGNORE
  }

  InstanceDiedPolicy onInstanceDied(WeaveRunnableSpecification weaveSpec);
}
