package com.continuuity.zk;

import com.continuuity.internal.zk.DefaultZKClientService;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Service;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import javax.annotation.Nullable;

/**
 * ZooKeeper client service that provides asynchronous zookeeper operations. The {@link #start()} method needed
 * to be called before calling any other method on this interface. When the client is no longer needed, call
 * {@link #stop()} to release any resources that it holds.
 */
public interface ZKClientService extends Service {

  /**
   * Same as calling
   * {@link #create(String, byte[], org.apache.zookeeper.CreateMode, boolean) create(path, data, createMode, true)}.
   *
   * @see #create(String, byte[], org.apache.zookeeper.CreateMode, boolean)
   */
  OperationFuture<String> create(String path, @Nullable byte[] data, CreateMode createMode);

  /**
   * Creates a path in zookeeper, with given data and create mode.
   *
   * @param path Path to be created
   * @param data Data to be stored in the node, or {@code null} if no data to store.
   * @param createMode The {@link org.apache.zookeeper.CreateMode} for the node.
   * @param createParent If {@code true} and parent nodes are missing, it will create all parent nodes as normal
   *                     persistent node before creating the request node.
   * @return A {@link OperationFuture} that will be completed when the
   *         creation is done. If there is error during creation, it will be reflected as error in the future.
   */
  OperationFuture<String> create(String path, @Nullable byte[] data, CreateMode createMode, boolean createParent);

  /**
   * Checks if the path exists. Same as calling
   * {@link #exists(String, org.apache.zookeeper.Watcher) exists(path, null)}.
   *
   * @see #exists(String, org.apache.zookeeper.Watcher)
   */
  OperationFuture<Stat> exists(String path);

  /**
   * Checks if the given path exists and leave a watcher on the node for watching creation/deletion/data changes
   * on the node.
   *
   * @param path The path to check for existence.
   * @param watcher Watcher for watching changes, or {@code null} if no watcher to set.
   * @return A {@link OperationFuture} that will be completed when the exists check is done. If the path
   *         does exists, the node {@link Stat} is set into the future. If the path doesn't exists,
   *         a {@code null} value is set into the future.
   */
  OperationFuture<Stat> exists(String path, @Nullable Watcher watcher);

  /**
   * Gets the list of children nodes under the given path. Same as calling
   * {@link #getChildren(String, org.apache.zookeeper.Watcher) getChildren(path, null)}.
   *
   * @see #getChildren(String, org.apache.zookeeper.Watcher)
   */
  OperationFuture<NodeChildren> getChildren(String path);

  /**
   * Gets the list of children nodes under the given path and leave a watcher on the node for watching node
   * deletion and children nodes creation/deletion.
   *
   * @param path The path to fetch for children nodes
   * @param watcher Watcher for watching changes, or {@code null} if no watcher to set.
   * @return A {@link OperationFuture} that will be completed when the getChildren call is done, with the result
   *         given as {@link NodeChildren}. If there is error, it will be reflected as error in the future.
   */
  OperationFuture<NodeChildren> getChildren(String path, @Nullable Watcher watcher);

  /**
   * Gets the data stored in the given path. Same as calling
   * {@link #getData(String, org.apache.zookeeper.Watcher) getData(path, null)}.
   */
  OperationFuture<NodeData> getData(String path);

  /**
   * Gets the data stored in the given path and leave a watcher on the node for watching deletion/data changes on
   * the node.
   *
   * @param path The path to get data from.
   * @param watcher Watcher for watching changes, or {@code null} if no watcher to set.
   * @return A {@link OperationFuture} that will be completed when the getData call is done, with the result
   *         given as {@link NodeData}. If there is error, it will be reflected as error in the future.
   */
  OperationFuture<NodeData> getData(String path, @Nullable Watcher watcher);

  /**
   * Sets the data for the given path without matching version. Same as calling
   * {@link #setData(String, byte[], int) setData(path, data, -1)}.
   */
  OperationFuture<Stat> setData(String path, byte[] data);

  /**
   * Sets the data for the given path that match the given version. If the version given is {@code -1}, it matches
   * any version.
   *
   * @param dataPath The path to set data to.
   * @param data Data to be set.
   * @param version Matching version.
   * @return A {@link OperationFuture} that will be completed when the setData call is done, with node {@link Stat}
   *         given as the future result. If there is error, it will be reflected as error in the future.
   */
  OperationFuture<Stat> setData(String dataPath, byte[] data, int version);

  /**
   * Deletes the node of the given path without matching version. Same as calling
   * {@link #delete(String, int) delete(path, -1)}.
   *
   * @see #delete(String, int)
   */
  OperationFuture<String> delete(String path);

  /**
   * Deletes the node of the given path that match the given version. If the version given is {@code -1}, it matches
   * any version.
   *
   * @param deletePath The path to set data to.
   * @param version Matching version.
   * @return A {@link OperationFuture} that will be completed when the setData call is done, with node path
   *         given as the future result. If there is error, it will be reflected as error in the future.
   */
  OperationFuture<String> delete(String deletePath, int version);

  /**
   * Returns a {@link Supplier} of {@link ZooKeeper} that gives the current {@link ZooKeeper} in use at the moment
   * when {@link com.google.common.base.Supplier#get()} get called.
   *
   * @return A {@link Supplier Supplier&lt;ZooKeeper&gt;}
   */
  Supplier<ZooKeeper> getZooKeeperSupplier();

  /**
   * Builder for creating an implementation of {@link ZKClientService}.
   * The default client timeout is 4000ms.
   */
  public static final class Builder {

    private final String connectStr;
    private int timeout = 4000;
    private Watcher connectionWatcher;

    /**
     * Creates a {@link Builder} with the given ZooKeeper connection string.
     * @param connectStr The connection string.
     * @return A new instance of Builder.
     */
    public static Builder of(String connectStr) {
      return new Builder(connectStr);
    }

    /**
     * Sets the client timeout to the give milliseconds.
     * @param timeout timeout in milliseconds.
     * @return This builder
     */
    public Builder setSessionTimeout(int timeout) {
      this.timeout = timeout;
      return this;
    }

    /**
     * Sets a {@link Watcher} that will be called whenever connection state change.
     * @param watcher The watcher to set.
     * @return This builder.
     */
    public Builder setConnectionWatcher(Watcher watcher) {
      this.connectionWatcher = watcher;
      return this;
    }

    /**
     * Creates an instance of {@link ZKClientService} with the settings of this builder.
     * @return A new instance of {@link ZKClientService}.
     */
    public ZKClientService build() {
      return new DefaultZKClientService(connectStr, timeout, connectionWatcher);
    }

    private Builder(String connectStr) {
      this.connectStr = connectStr;
    }
  }
}
