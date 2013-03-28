package com.continuuity.weave.api;

import javax.annotation.Nullable;
import java.net.URI;

/**
 *
 */
public interface LocalFile {

  String getName();

  URI getURI();

  boolean isArchive();

  @Nullable
  String getPattern();
}
