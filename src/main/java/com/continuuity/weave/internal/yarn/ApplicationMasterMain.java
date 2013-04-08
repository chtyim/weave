package com.continuuity.weave.internal.yarn;

import com.continuuity.weave.internal.ServiceMain;

import java.io.File;
import java.util.concurrent.ExecutionException;

/**
 * Main class for launching {@link ApplicationMasterService}. It takes single argument, which is the
 * zookeeper connection string.
 */
public final class ApplicationMasterMain extends ServiceMain {

  /**
   * Starts the application master.
   * @param args args[0] - ZooKeeper connection string. args[1] - local resource name for spec file.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws Exception {
    new ApplicationMasterMain().doMain(new ApplicationMasterService(args[0], new File(args[1])));
  }
}
