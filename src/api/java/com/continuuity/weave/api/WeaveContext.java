package com.continuuity.weave.api;

import java.net.InetAddress;

/**
 *
 */
public interface WeaveContext {

  InetAddress getHost();

  WeaveRunnableSpecification getSpecification();
}
