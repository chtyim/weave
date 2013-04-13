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
package com.continuuity.kafka.client;

import com.continuuity.internal.kafka.client.Compression;
import com.google.common.util.concurrent.Service;

import java.util.Iterator;

/**
 *
 */
public interface KafkaClient extends Service {

  PreparePublish preparePublish(String topic, Compression compression);

  Iterator<FetchedMessage> consume(String topic, int partition, long offset, int maxSize);
}
