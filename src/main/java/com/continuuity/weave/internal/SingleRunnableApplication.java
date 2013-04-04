package com.continuuity.weave.internal;

import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveRunnable;
import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.continuuity.weave.api.WeaveSpecification;

/**
 *
 */
public class SingleRunnableApplication implements WeaveApplication {

  private final WeaveRunnable runnable;
  private final ResourceSpecification resourceSpec;

  public SingleRunnableApplication(WeaveRunnable runnable, ResourceSpecification resourceSpec) {
    this.runnable = runnable;
    this.resourceSpec = resourceSpec;
  }

  @Override
  public WeaveSpecification configure() {
    WeaveRunnableSpecification runnableSpec = runnable.configure();
    return WeaveSpecification.Builder.with()
      .setName(runnableSpec.getName())
      .withRunnable().add(runnable, resourceSpec)
      .noLocalFiles()
      .build();
  }
}
