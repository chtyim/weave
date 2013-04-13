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
package com.continuuity.weave.api;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 *
 */
public abstract class AbstractWeaveRunnable implements WeaveRunnable {

  private Map<String, String> args;

  protected AbstractWeaveRunnable() {
    this.args = ImmutableMap.of();
  }

  protected AbstractWeaveRunnable(Map<String, String> args) {
    this.args = ImmutableMap.copyOf(args);
  }

  @Override
  public WeaveRunnableSpecification configure() {
    return WeaveRunnableSpecification.Builder.with()
      .setName(getClass().getSimpleName())
      .withArguments(args)
      .build();
  }

  @Override
  public void initialize(WeaveContext context) {
    this.args = context.getSpecification().getArguments();
  }

  @Override
  public void handleCommand(Command command) throws Exception {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  protected Map<String, String> getArguments() {
    return args;
  }

  protected String getArgument(String key) {
    return args.get(key);
  }
}
