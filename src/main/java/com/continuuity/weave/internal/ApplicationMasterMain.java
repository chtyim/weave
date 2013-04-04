package com.continuuity.weave.internal;

import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.internal.json.WeaveSpecificationAdapter;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.concurrent.ExecutionException;

/**
 * Main class for launching {@link ApplicationMasterService}. It takes single argument, which is the
 * zookeeper connection string.
 */
public final class ApplicationMasterMain {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationMasterMain.class);

  /**
   * Starts the application master.
   * @param args args[0] - ZooKeeper connection string. args[1] - local resource name for spec file.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws Exception {
    final ApplicationMasterService appMaster = new ApplicationMasterService(args[0],
                                                                            loadWeaveSpec(args[1]),
                                                                            new File(args[1]));

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOG.info("Shutdown hook triggered");
        appMaster.stopAndWait();
      }
    });

    // Starts the app master
    appMaster.start();

    // Listen for application master service completion
    final SettableFuture<Service.State> completion = SettableFuture.create();
    appMaster.addListener(new Service.Listener() {
      @Override
      public void starting() {
        LOG.info("Starting application master");
      }

      @Override
      public void running() {
        LOG.info("Application master running");
      }

      @Override
      public void stopping(Service.State from) {
        LOG.info("Stopping application master from " + from);
      }

      @Override
      public void terminated(Service.State from) {
        LOG.info("Application master terminated from " + from);
        completion.set(from);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        LOG.info("Application master failed from " + from, failure);
        completion.setException(failure);
      }
    }, MoreExecutors.sameThreadExecutor());

    // If app master failed with exception, the future.get() will throws exception
    completion.get();
  }

  private static WeaveSpecification loadWeaveSpec(String spec) throws IOException{
    Reader reader = Files.newReader(new File(spec), Charsets.UTF_8);
    try {
      return WeaveSpecificationAdapter.create().fromJson(reader);
    } finally {
      reader.close();
    }
  }
}
