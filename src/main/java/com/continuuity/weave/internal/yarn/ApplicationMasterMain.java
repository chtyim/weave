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

import com.continuuity.weave.internal.ServiceMain;

import java.io.File;
import java.util.concurrent.ExecutionException;

/**
 * Main class for launching {@link ApplicationMasterService}. It takes single argument, which is the
 * zookeeper connection string.
 */
public final class ApplicationMasterMain extends ServiceMain {

  /**
   * Starts the application master.
   * @param args args[0] - ZooKeeper connection string. args[1] - local resource name for spec file.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws Exception {
    new ApplicationMasterMain().doMain(new ApplicationMasterService(args[0], new File(args[1])));
  }
}
