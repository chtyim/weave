package com.continuuity.weave.internal.container;

import com.continuuity.weave.api.WeaveRunnableSpecification;
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

/**
 *
 */
public class WeaveContainerMain {

  private static final Logger LOG = LoggerFactory.getLogger(WeaveContainerMain.class);

  /**
   *
   * @param args 0 - zkStr, 1 - spec.json, 2 - runnable name
   * @throws Exception
   */
  public static void main(final String[] args) throws Exception {
    // TODO: Use Jar class loader
    WeaveSpecification weaveSpec = loadWeaveSpec(args[1]);
    final WeaveRunnableSpecification runnableSpec = weaveSpec.getRunnables().get(args[2]).getRunnableSpecification();

    final WeaveContainer container = new WeaveContainer(args[0], runnableSpec, ClassLoader.getSystemClassLoader());

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOG.info("Shutdown hook triggered");
        container.stopAndWait();
      }
    });

    container.start();
    final SettableFuture<Service.State> completion = SettableFuture.create();
    container.addListener(new Service.Listener() {
      @Override
      public void starting() {
        LOG.info("Starting container");
      }

      @Override
      public void running() {
        LOG.info("Container running: class=" + runnableSpec.getClassName() + ", name=" + runnableSpec.getName());
      }

      @Override
      public void stopping(Service.State from) {
        LOG.info("Stopping container from " + from);
      }

      @Override
      public void terminated(Service.State from) {
        LOG.info("Container terminated from " + from);
        completion.set(from);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        LOG.info("Container failed from " + from, failure);
        completion.setException(failure);
      }
    }, MoreExecutors.sameThreadExecutor());

    // If container failed with exception, the future.get() will throws exception
    completion.get();
  }

  private static WeaveSpecification loadWeaveSpec(String spec) throws IOException {
    Reader reader = Files.newReader(new File(spec), Charsets.UTF_8);
    try {
      return WeaveSpecificationAdapter.create().fromJson(reader);
    } finally {
      reader.close();
    }
  }
}
