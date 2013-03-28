package com.continuuity.weave.zk;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.concurrent.Executor;

/**
 *
 */
public final class InMemoryZKServer implements Service {

  private final File dataDir;
  private final int tickTime;
  private final boolean autoClean;
  private final int port;
  private final Service delegateService = new AbstractIdleService() {
    @Override
    protected void startUp() throws Exception {
      ZooKeeperServer zkServer = new ZooKeeperServer();
      FileTxnSnapLog ftxn = new FileTxnSnapLog(dataDir, dataDir);
      zkServer.setTxnLogFactory(ftxn);
      zkServer.setTickTime(tickTime);

      factory = ServerCnxnFactory.createFactory();
      factory.configure(getAddress(port), -1);
      factory.startup(zkServer);
    }

    @Override
    protected void shutDown() throws Exception {
      try {
        factory.shutdown();
      } finally {
        if (autoClean) {
          cleanDir(dataDir);
        }
      }
    }
  };

  private ServerCnxnFactory factory;

  public static Builder builder() {
    return new Builder();
  }

  private InMemoryZKServer(File dataDir, int tickTime, boolean autoClean, int port) {
    if (dataDir == null) {
      dataDir = Files.createTempDir();
      autoClean = true;
    } else {
      Preconditions.checkArgument(dataDir.isDirectory() || dataDir.mkdirs() || dataDir.isDirectory());
    }

    this.dataDir = dataDir;
    this.tickTime = tickTime;
    this.autoClean = autoClean;
    this.port = port;
  }

  public String getConnectionStr() {
    InetSocketAddress addr = factory.getLocalAddress();
    return String.format("%s:%d", addr.getHostName(), addr.getPort());
  }

  public InetSocketAddress getLocalAddress() {
    return factory.getLocalAddress();
  }

  private InetSocketAddress getAddress(int port) {
    if (port <= 0) {
      try {
        ServerSocket ss = new ServerSocket(0);
        try {
          port = ss.getLocalPort();
        } finally {
          ss.close();
        }
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    try {
      return new InetSocketAddress(InetAddress.getLocalHost(), port);
    } catch (UnknownHostException e) {
      throw Throwables.propagate(e);
    }
  }

  private void cleanDir(File dir) {
    File[] files = dir.listFiles();
    if (files == null) {
      return;
    }
    for (File file : files) {
      if (file.isDirectory()) {
        cleanDir(file);
      }
      file.delete();
    }
  }

  @Override
  public ListenableFuture<State> start() {
    return delegateService.start();
  }

  @Override
  public State startAndWait() {
    return delegateService.startAndWait();
  }

  @Override
  public boolean isRunning() {
    return delegateService.isRunning();
  }

  @Override
  public State state() {
    return delegateService.state();
  }

  @Override
  public ListenableFuture<State> stop() {
    return delegateService.stop();
  }

  @Override
  public State stopAndWait() {
    return delegateService.stopAndWait();
  }

  @Override
  public void addListener(Listener listener, Executor executor) {
    delegateService.addListener(listener, executor);
  }

  public static final class Builder {
    private File dataDir;
    private boolean autoCleanDataDir = false;
    private int tickTime = ZooKeeperServer.DEFAULT_TICK_TIME;
    private int port = -1;

    public Builder setDataDir(File dataDir) {
      this.dataDir = dataDir;
      return this;
    }

    public Builder setAutoCleanDataDir(boolean auto) {
      this.autoCleanDataDir = auto;
      return this;
    }

    public Builder setTickTime(int tickTime) {
      this.tickTime = tickTime;
      return this;
    }

    public Builder setPort(int port) {
      this.port = port;
      return this;
    }

    public InMemoryZKServer build() {
      return new InMemoryZKServer(dataDir, tickTime, autoCleanDataDir, port);
    }

    private Builder() {
    }
  }
}
