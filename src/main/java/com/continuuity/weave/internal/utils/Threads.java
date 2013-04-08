package com.continuuity.weave.internal.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ThreadFactory;

/**
 *
 */
public final class Threads {

  /**
   * Handy method to create {@link ThreadFactory} that creates daemon threads with the given name format.
   *
   * @param nameFormat Name format for the thread names
   * @return A {@link ThreadFactory}.
   * @see ThreadFactoryBuilder
   */
  public static ThreadFactory createDaemonThreadFactory(String nameFormat) {
    return new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat(nameFormat)
      .build();
  }

  private Threads() {
  }
}
