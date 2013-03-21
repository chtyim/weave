package com.continuuity.weave.api;

import java.util.Map;

/**
 *
 */
public interface WeaveSpecification {

  String getClassName();

  String getName();

  Map<String, String> getArguments();
}
