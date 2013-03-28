package com.continuuity.weave.internal.api;

import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.google.common.collect.ImmutableList;

import java.util.Collection;

/**
 *
 */
public final class DefaultRuntimeSpecification implements RuntimeSpecification {

  private final String name;
  private final WeaveRunnableSpecification runnableSpec;
  private final ResourceSpecification resourceSpec;
  private final Collection<LocalFile> localFiles;

  public DefaultRuntimeSpecification(String name,
                                     WeaveRunnableSpecification runnableSpec,
                                     ResourceSpecification resourceSpec,
                                     Collection<LocalFile> localFiles) {
    this.name = name;
    this.runnableSpec = runnableSpec;
    this.resourceSpec = resourceSpec;
    this.localFiles = ImmutableList.copyOf(localFiles);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public WeaveRunnableSpecification getRunnableSpecification() {
    return runnableSpec;
  }

  @Override
  public ResourceSpecification getResourceSpecification() {
    return resourceSpec;
  }

  @Override
  public Collection<LocalFile> getLocalFiles() {
    return localFiles;
  }
}
