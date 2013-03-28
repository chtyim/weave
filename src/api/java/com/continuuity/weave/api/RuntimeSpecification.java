package com.continuuity.weave.api;

import java.util.Collection;

/**
 *
 */
public interface RuntimeSpecification {

  String getName();

  WeaveRunnableSpecification getRunnableSpecification();

  ResourceSpecification getResourceSpecification();

  Collection<LocalFile> getLocalFiles();
}
