package com.continuuity.weave.internal.api;

import com.continuuity.weave.api.LocalFile;

import javax.annotation.Nullable;
import java.net.URI;

/**
 *
 */
public final class DefaultLocalFile implements LocalFile {

  private final String name;
  private final URI uri;
  private final boolean archive;
  private final String pattern;

  public DefaultLocalFile(String name, URI uri, boolean archive, @Nullable String pattern) {
    this.name = name;
    this.uri = uri;
    this.archive = archive;
    this.pattern = pattern;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public URI getURI() {
    return uri;
  }

  @Override
  public boolean isArchive() {
    return archive;
  }

  @Override
  public String getPattern() {
    return pattern;
  }
}
