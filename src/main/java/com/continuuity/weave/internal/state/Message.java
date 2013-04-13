package com.continuuity.weave.internal.state;

import com.continuuity.weave.api.Command;

/**
 *
 */
public interface Message {

  enum Scope {
    APPLICATION,
    ALL_RUNNABLE,
    RUNNABLE
  }

  String getId();

  Scope getScope();

  /**
   * @return the name of the target runnable if scope is {@link Scope#RUNNABLE} or {@code null} otherwise.
   */
  String getRunnableName();

  Command getCommand();
}
