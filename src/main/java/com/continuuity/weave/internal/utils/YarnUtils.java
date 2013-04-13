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
package com.continuuity.weave.internal.utils;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;

/**
 * Collection of helper methods to simplify YARN calls
 */
public class YarnUtils {

  public static LocalResource createLocalResource(LocalResourceType type, File file) {
    LocalResource resource = Records.newRecord(LocalResource.class);
    resource.setVisibility(LocalResourceVisibility.APPLICATION);
    resource.setType(type);
    resource.setResource(ConverterUtils.getYarnUrlFromURI(file.toURI()));
    resource.setTimestamp(file.lastModified());
    resource.setSize(file.length());
    return resource;
  }

  private YarnUtils() {
  }
}
