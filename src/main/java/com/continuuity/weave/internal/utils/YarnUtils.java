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
