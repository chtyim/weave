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
package com.continuuity.zk;

import com.continuuity.internal.zk.FailureRetryZKClientService;
import com.continuuity.internal.zk.RewatchOnExpireZKClientService;

/**
 * Provides static factory method to create {@link ZKClientService} with modified behaviors.
 */
public final class ZKClientServices {

  /**
   * Creates a {@link ZKClientService} that will perform auto re-watch on all existing watches
   * when reconnection happens after session expiration. All {@link org.apache.zookeeper.Watcher Watchers}
   * set through the returned {@link ZKClientService} would not receive any connection events.
   *
   * @param clientService The {@link ZKClientService} for operations delegation.
   * @return A {@link ZKClientService} that will do auto re-watch on all methods that accept a
   *        {@link org.apache.zookeeper.Watcher} upon session expiration.
   */
  public static ZKClientService reWatchOnExpire(ZKClientService clientService) {
    return new RewatchOnExpireZKClientService(clientService);
  }

  /**
   * Creates a {@link ZKClientService} that will retry interim failure (e.g. connection loss, session expiration)
   * based on the given {@link RetryStrategy}.
   *
   * @param clientService The {@link ZKClientService} for operations delegation.
   * @param retryStrategy The {@link RetryStrategy} to be invoke when there is operation failure.
   * @return A {@link ZKClientService}.
   */
  public static ZKClientService retryOnFailure(ZKClientService clientService, RetryStrategy retryStrategy) {
    return new FailureRetryZKClientService(clientService, retryStrategy);
  }

  private ZKClientServices() {
  }
}
