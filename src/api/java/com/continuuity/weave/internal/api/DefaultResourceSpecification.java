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
package com.continuuity.weave.internal.api;

import com.continuuity.weave.api.ResourceSpecification;

/**
*
*/
public final class DefaultResourceSpecification implements ResourceSpecification {
  private final int cores;
  private final int memorySize;
  private final int uplink;
  private final int downlink;

  public DefaultResourceSpecification(int cores, int memorySize, int uplink, int downlink) {
    this.cores = cores;
    this.memorySize = memorySize;
    this.uplink = uplink;
    this.downlink = downlink;
  }

  @Override
  public int getCores() {
    return cores;
  }

  @Override
  public int getMemorySize() {
    return memorySize;
  }

  @Override
  public int getUplink() {
    return uplink;
  }

  @Override
  public int getDownlink() {
    return downlink;
  }
}
