package com.continuuity.weave.internal;

import com.continuuity.weave.api.WeaveApplicationSpecification;
import com.continuuity.weave.api.WeaveSpecification;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Main class for launching {@link ApplicationMasterService}. It takes single argument, which is the
 * zookeeper connection string.
 */
public final class ApplicationMasterMain {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationMasterMain.class);

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    WeaveApplicationSpecification appSpec = loadApplicationSpecification();
    final ApplicationMasterService appMaster = new ApplicationMasterService(args[0], appSpec);

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

  private static WeaveApplicationSpecification loadApplicationSpecification() {
    // TODO: Suppose loading the WeaveApplicationSpecification from file.
    return new WeaveApplicationSpecification() {
      @Override
      public String getName() {
        return "Testing";
      }

      @Override
      public Map<String, WeaveSpecification> getRunnables() {
        return ImmutableMap.of();
      }
    };
  }
}
