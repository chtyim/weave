package com.continuuity.weave.internal;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

/**
 * Class for the static main method that starts a service.
 */
public abstract class ServiceMain {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceMain.class);

  protected final void doMain(final Service service) throws ExecutionException, InterruptedException {
    final String serviceName = service.getClass().getName();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOG.info("Shutdown hook triggered. Shutting down service " + serviceName);
        service.stopAndWait();
        LOG.info("Service shutdown " + serviceName);
      }
    });

    // Listener for state changes of the service
    final SettableFuture<Service.State> completion = SettableFuture.create();
    service.addListener(new Service.Listener() {
      @Override
      public void starting() {
        LOG.info("Starting service " + serviceName);
      }

      @Override
      public void running() {
        LOG.info("Service running " + serviceName);
      }

      @Override
      public void stopping(Service.State from) {
        LOG.info("Stopping service " + serviceName + " from " + from);
      }

      @Override
      public void terminated(Service.State from) {
        LOG.info("Service terminated " + serviceName + " from " + from);
        completion.set(from);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        LOG.info("Service failure " + serviceName, failure);
        completion.setException(failure);
      }
    }, MoreExecutors.sameThreadExecutor());

    // Starts the service
    service.start();

    // If container failed with exception, the future.get() will throws exception
    completion.get();

  }
}
