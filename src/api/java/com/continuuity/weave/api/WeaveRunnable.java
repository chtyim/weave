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

/**
 * The <code>WeaveRunnable</code> interface should be implemented by any
 * class whose instances are intended to be executed in a Weave cluster.
 */
public interface WeaveRunnable extends Runnable {

  /**
   * Called at the submission time. Executed on the client side.
   * @return A {@link WeaveRunnableSpecification} built by {@link WeaveRunnableSpecification.Builder}.
   */
  WeaveRunnableSpecification configure();

  /**
   * Called when the container process starts. Executed in container machine.
   * @param context Contains information about the runtime context.
   */
  void initialize(WeaveContext context);

  /**
   * Called when a command is received. A normal return denotes the command has been processed successfully, otherwise
   * {@link Exception} should be thrown.
   * @param command Contains details of the command.
   * @throws Exception
   */
  void handleCommand(Command command) throws Exception;

  /**
   * Requests to stop the running service.
   */
  void stop();
}
