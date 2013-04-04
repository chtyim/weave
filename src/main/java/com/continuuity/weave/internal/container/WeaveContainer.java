package com.continuuity.weave.internal.container;

import com.continuuity.weave.api.WeaveContext;
import com.continuuity.weave.api.WeaveRunnable;
import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.continuuity.weave.internal.utils.Instances;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * This class act as a yarn container and run a {@link WeaveRunnable}.
 */
public final class WeaveContainer extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(WeaveContainer.class);

  private final WeaveRunnableSpecification specification;
  private final ClassLoader classLoader;
  private final String zkConnectionStr;
  private final WeaveContext context;
  private WeaveRunnable runnable;
//  private ZooKeeper zooKeeper;
  private volatile Thread runThread;

  public WeaveContainer(String zkConnectionStr,
                        WeaveRunnableSpecification specification,
                        ClassLoader classLoader) throws UnknownHostException {
    this.specification = specification;
    this.classLoader = classLoader;
    this.zkConnectionStr = zkConnectionStr;

    final InetAddress hostname = InetAddress.getByName(System.getenv("CONTAINER_HOST"));
    this.context = new WeaveContext() {
      @Override
      public InetAddress getHost() {
        return hostname;
      }

      @Override
      public WeaveRunnableSpecification getSpecification() {
        return WeaveContainer.this.specification;
      }
    };
  }

  @Override
  protected void startUp() throws Exception {
    Class<?> runnableClass = classLoader.loadClass(specification.getClassName());
    Preconditions.checkArgument(WeaveRunnable.class.isAssignableFrom(runnableClass),
                                "Class %s is not instance of WeaveRunnable.", specification.getClassName());

    runnable = Instances.newInstance((Class<WeaveRunnable>) runnableClass);
    runnable.initialize(context);
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      runnable.stop();
    } catch (Throwable t) {
      LOG.error("Exception when stopping runnable.", t);
    } finally {
//      zooKeeper.close();
    }
  }

  @Override
  protected void triggerShutdown() {
    Thread runThread = this.runThread;
    if (runThread != null) {
      runThread.interrupt();
    }
  }

  @Override
  protected void run() throws Exception {
    runThread = Thread.currentThread();
    runnable.run();
  }
}
