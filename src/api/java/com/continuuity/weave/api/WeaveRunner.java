package com.continuuity.weave.api;

/**
 *
 */
public interface WeaveRunner {

  WeavePreparer prepare(WeaveRunnable runnable);

  WeavePreparer prepare(WeaveRunnable runnable, ResourceSpecification resourceSpecification);

  WeavePreparer prepare(WeaveApplication application);

  WeaveController lookup(RunId runId);
}
