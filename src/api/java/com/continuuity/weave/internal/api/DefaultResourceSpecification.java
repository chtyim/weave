package com.continuuity.weave.internal.api;

import com.continuuity.weave.api.ResourceSpecification;

/**
*
*/
public final class DefaultResourceSpecification implements ResourceSpecification {
  private final int cores;
  private final int memorySize;
  private final int uplink;
  private final int downlink;

  public DefaultResourceSpecification(int cores, int memorySize, int uplink, int downlink) {
    this.cores = cores;
    this.memorySize = memorySize;
    this.uplink = uplink;
    this.downlink = downlink;
  }

  @Override
  public int getCores() {
    return cores;
  }

  @Override
  public int getMemorySize() {
    return memorySize;
  }

  @Override
  public int getUplink() {
    return uplink;
  }

  @Override
  public int getDownlink() {
    return downlink;
  }
}
