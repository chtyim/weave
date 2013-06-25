/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.continuuity.weave.internal;

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.ServiceController;
import com.continuuity.weave.common.Threads;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public abstract class AbstractExecutionServiceController implements ServiceController {

  private final RunId runId;
  private final ListenerExecutors listenerExecutors;
  private final Service serviceDelegate;

  protected AbstractExecutionServiceController(RunId runId) {
    this.runId = runId;
    this.listenerExecutors = new ListenerExecutors();
    this.serviceDelegate = new ServiceDelegate();
  }

  protected abstract void startUp();

  protected abstract void shutDown();

  @Override
  public final RunId getRunId() {
    return runId;
  }

  @Override
  public final void addListener(Listener listener, Executor executor) {
    listenerExecutors.addListener(new ListenerExecutor(listener, executor));
  }

  @Override
  public final ListenableFuture<State> start() {
    addListener(listenerExecutors, Threads.SAME_THREAD_EXECUTOR);
    return serviceDelegate.start();
  }

  @Override
  public final State startAndWait() {
    return Futures.getUnchecked(start());
  }

  @Override
  public final boolean isRunning() {
    return serviceDelegate.isRunning();
  }

  @Override
  public final State state() {
    return serviceDelegate.state();
  }

  @Override
  public final State stopAndWait() {
    return Futures.getUnchecked(stop());
  }

  @Override
  public final ListenableFuture<State> stop() {
    return serviceDelegate.stop();
  }

  protected Executor executor(final State state) {
    return new Executor() {
      @Override
      public void execute(Runnable command) {
        Thread t = new Thread(command, getClass().getSimpleName() + " " + state);
        t.setDaemon(true);
        t.start();
      }
    };
  }


  private final class ServiceDelegate extends AbstractIdleService {
    @Override
    protected void startUp() throws Exception {
      AbstractExecutionServiceController.this.startUp();
    }

    @Override
    protected void shutDown() throws Exception {
      AbstractExecutionServiceController.this.shutDown();
    }

    @Override
    protected Executor executor(State state) {
      return AbstractExecutionServiceController.this.executor(state);
    }
  }

  /**
   * Inner class for dispatching listener call back to a list of listeners
   */
  private static final class ListenerExecutors implements Listener {

    private interface Callback {
      void call(Listener listener);
    }

    private final Queue<ListenerExecutor> listeners = new ConcurrentLinkedQueue<ListenerExecutor>();
    private final AtomicReference<Callback> lastState = new AtomicReference<Callback>();

    private synchronized void addListener(final ListenerExecutor listener) {
      listeners.add(listener);
      Callback callback = lastState.get();
      callback.call(listener);
    }

    @Override
    public synchronized void starting() {
      lastState.set(new Callback() {
        @Override
        public void call(Listener listener) {
          listener.starting();
        }
      });
      for (ListenerExecutor listener : listeners) {
        listener.starting();
      }
    }

    @Override
    public synchronized void running() {
      lastState.set(new Callback() {
        @Override
        public void call(Listener listener) {
          listener.running();
        }
      });
      for (ListenerExecutor listener : listeners) {
        listener.running();
      }
    }

    @Override
    public synchronized void stopping(final State from) {
      lastState.set(new Callback() {
        @Override
        public void call(Listener listener) {
          listener.stopping(from);
        }
      });
      for (ListenerExecutor listener : listeners) {
        listener.stopping(from);
      }
    }

    @Override
    public synchronized void terminated(final State from) {
      lastState.set(new Callback() {
        @Override
        public void call(Listener listener) {
          listener.terminated(from);
        }
      });
      for (ListenerExecutor listener : listeners) {
        listener.terminated(from);
      }
    }

    @Override
    public synchronized void failed(final State from, final Throwable failure) {
      lastState.set(new Callback() {
        @Override
        public void call(Listener listener) {
          listener.failed(from, failure);
        }
      });
      for (ListenerExecutor listener : listeners) {
        listener.failed(from, failure);
      }
    }
  }
}
