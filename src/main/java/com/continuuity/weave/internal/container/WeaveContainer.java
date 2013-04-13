package com.continuuity.weave.internal.container;

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.WeaveContext;
import com.continuuity.weave.api.WeaveRunnable;
import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.continuuity.weave.internal.api.RunIds;
import com.continuuity.weave.internal.state.Message;
import com.continuuity.weave.internal.state.MessageCallback;
import com.continuuity.weave.internal.state.ZKServiceDecorator;
import com.continuuity.weave.internal.utils.Instances;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executor;

/**
 * This class act as a yarn container and run a {@link WeaveRunnable}.
 */
public final class WeaveContainer implements Service {

  private static final int ZK_TIMEOUT = 10000;    // 10 seconds
  private static final Logger LOG = LoggerFactory.getLogger(WeaveContainer.class);

  private final WeaveRunnableSpecification specification;
  private final ClassLoader classLoader;
  private final WeaveContext context;
  private final RunId runId;
  private final ZKServiceDecorator serviceDelegate;
  private final ContainerInfo containerInfo;
  private WeaveRunnable runnable;

  public WeaveContainer(String zkConnectionStr,
                        WeaveRunnableSpecification specification,
                        ClassLoader classLoader) throws UnknownHostException {
    this.specification = specification;
    this.classLoader = classLoader;
    this.runId = RunIds.generate();
    this.serviceDelegate = new ZKServiceDecorator(zkConnectionStr, ZK_TIMEOUT, runId,
                                                  createLiveNodeSupplier(), new ServiceDelegate());
    this.containerInfo = new ContainerInfo();
    this.context = new WeaveContext() {
      @Override
      public InetAddress getHost() {
        return containerInfo.getHost();
      }

      @Override
      public WeaveRunnableSpecification getSpecification() {
        return WeaveContainer.this.specification;
      }
    };
  }

  private ListenableFuture<String> processMessage(Message message) {
    SettableFuture<String> result = SettableFuture.create();

    return result;
  }

  private Supplier<? extends JsonElement> createLiveNodeSupplier() {
    return new Supplier<JsonElement>() {
      @Override
      public JsonElement get() {
        JsonObject jsonObj = new JsonObject();
        jsonObj.addProperty("containerId", containerInfo.getId());
        jsonObj.addProperty("host", containerInfo.getHost().getCanonicalHostName());
        return jsonObj;
      }
    };
  }

  @Override
  public ListenableFuture<State> start() {
    return serviceDelegate.start();
  }

  @Override
  public State startAndWait() {
    return Futures.getUnchecked(start());
  }

  @Override
  public boolean isRunning() {
    return serviceDelegate.isRunning();
  }

  @Override
  public State state() {
    return serviceDelegate.state();
  }

  @Override
  public ListenableFuture<State> stop() {
    return serviceDelegate.stop();
  }

  @Override
  public State stopAndWait() {
    return Futures.getUnchecked(stop());
  }

  @Override
  public void addListener(Listener listener, Executor executor) {
    serviceDelegate.addListener(listener, executor);
  }

  private final class ServiceDelegate extends AbstractExecutionThreadService implements MessageCallback {

    private volatile Thread runThread;

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

    @Override
    public ListenableFuture<String> onReceived(Message message) {
      return processMessage(message);
    }
  }

  private static final class ContainerInfo {
    private final String id;
    private final InetAddress host;
    private final int port;

    private ContainerInfo() throws UnknownHostException {
      id = System.getenv("YARN_CONTAINER_ID");
      host = InetAddress.getByName(System.getenv("YARN_CONTAINER_HOST"));
      port = Integer.parseInt(System.getenv("YARN_CONTAINER_PORT"));
    }

    private String getId() {
      return id;
    }

    private InetAddress getHost() {
      return host;
    }

    private int getPort() {
      return port;
    }
  }
}
