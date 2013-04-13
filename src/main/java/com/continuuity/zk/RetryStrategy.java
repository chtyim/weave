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

/**
 * Provides strategy to use for operation retries.
 */
public interface RetryStrategy {

  enum OperationType {
    CREATE,
    EXISTS,
    GET_CHILDREN,
    GET_DATA,
    SET_DATA,
    DELETE
  }

  /**
   * Returns the number of milliseconds to wait before retrying the operation.
   *
   * @param failureCount Number of times that the request has been failed.
   * @param startTime Timestamp in milliseconds that the request starts.
   * @param type Type of operation tried to perform.
   * @param path The path that the operation is acting on.
   * @return Number of milliseconds to wait before retrying the operation. Returning {@code 0} means
   *         retry it immediately, while negative means abort the operation.
   */
  long nextRetry(int failureCount, long startTime, OperationType type, String path);
}
