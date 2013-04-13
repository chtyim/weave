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
package com.continuuity.weave.internal.yarn;

import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveRunnable;
import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.continuuity.weave.api.WeaveSpecification;

/**
 *
 */
public class SingleRunnableApplication implements WeaveApplication {

  private final WeaveRunnable runnable;
  private final ResourceSpecification resourceSpec;

  public SingleRunnableApplication(WeaveRunnable runnable, ResourceSpecification resourceSpec) {
    this.runnable = runnable;
    this.resourceSpec = resourceSpec;
  }

  @Override
  public WeaveSpecification configure() {
    WeaveRunnableSpecification runnableSpec = runnable.configure();
    return WeaveSpecification.Builder.with()
      .setName(runnableSpec.getName())
      .withRunnable().add(runnableSpec.getName(), runnable, resourceSpec)
      .noLocalFiles()
      .anyOrder()
      .build();
  }
}
