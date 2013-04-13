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
package com.continuuity.internal.kafka.client;

/**
*
*/
public enum Compression {
  NONE(0),
  GZIP(1),
  SNAPPY(2);

  private final int code;

  Compression(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public static Compression fromCode(int code) {
    switch (code) {
      case 0:
        return NONE;
      case 1:
        return GZIP;
      case 2:
        return SNAPPY;
    }
    throw new IllegalArgumentException("Unknown compression code.");
  }
}
