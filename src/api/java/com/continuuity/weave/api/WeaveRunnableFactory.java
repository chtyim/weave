package com.continuuity.weave.api;

/**
 *
 */
public interface WeaveRunnableFactory {

  <T extends WeaveRunnable> T create(WeaveRunnableSpecification weaveRunnableSpecification);
}
