/**
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
