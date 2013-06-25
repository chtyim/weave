/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.continuuity.weave.internal.state;

import com.continuuity.weave.api.ServiceController;
import com.google.common.util.concurrent.Service;

/**
 *
 */
public final class StateNode {

  private final ServiceController.State state;
  private final String errorMessage;
  private final StackTraceElement[] stackTraces;

  /**
   * Constructs a StateNode with the given state.
   */
  public StateNode(ServiceController.State state) {
    this(state, null, null);
  }

  /**
   * Constructs a StateNode with {@link ServiceController.State#FAILED} caused by the given error.
   */
  public StateNode(Throwable error) {
    this(Service.State.FAILED, error.getMessage(), error.getStackTrace());
  }

  /**
   * Constructs a StateNode with the given state, error and stacktraces.
   * This constructor should only be used by the StateNodeCodec.
   */
  public StateNode(ServiceController.State state, String errorMessage, StackTraceElement[] stackTraces) {
    this.state = state;
    this.errorMessage = errorMessage;
    this.stackTraces = stackTraces;
  }

  public ServiceController.State getState() {
    return state;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public StackTraceElement[] getStackTraces() {
    return stackTraces;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("state=").append(state);

    if (errorMessage != null) {
      builder.append("\n").append("error=").append(errorMessage);
    }
    if (stackTraces != null) {
      builder.append("\n");
      for (StackTraceElement stackTrace : stackTraces) {
        builder.append("\tat ").append(stackTrace.toString()).append("\n");
      }
    }
    return builder.toString();
  }

}
