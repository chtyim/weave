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

import org.jboss.netty.buffer.ChannelBuffer;

/**
 *
 */
final class KafkaResponse {

  enum ErrorCode {
    UNKNOWN(-1),
    OK(0),
    OFFSET_OUT_OF_RANGE(1),
    INVALID_MESSAGE(2),
    WRONG_PARTITION(3),
    INVALID_FETCH_SIZE(4);

    private final int code;

    ErrorCode(int code) {
      this.code = code;
    }

    public int getCode() {
      return code;
    }

    public static ErrorCode fromCode(int code) {
      switch (code) {
        case -1:
          return UNKNOWN;
        case 0:
          return OK;
        case 1:
          return OFFSET_OUT_OF_RANGE;
        case 2:
          return INVALID_MESSAGE;
        case 3:
          return WRONG_PARTITION;
        case 4:
          return INVALID_FETCH_SIZE;
      }
      throw new IllegalArgumentException("Unknown error code");
    }
  }

  private final ErrorCode errorCode;
  private final ChannelBuffer body;
  private final int size;

  KafkaResponse(ErrorCode errorCode, ChannelBuffer body, int size) {
    this.errorCode = errorCode;
    this.body = body;
    this.size = size;
  }

  public int getSize() {
    return size;
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }

  public ChannelBuffer getBody() {
    return body;
  }
}
