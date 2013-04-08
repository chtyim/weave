package com.continuuity.weave.api;

import com.continuuity.weave.internal.api.DefaultLocalFile;
import com.continuuity.weave.internal.api.DefaultRuntimeSpecification;
import com.continuuity.weave.internal.api.DefaultWeaveRunnableSpecification;
import com.continuuity.weave.internal.api.DefaultWeaveSpecification;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.File;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public interface WeaveSpecification {

  interface Order {

    enum Type {
      STARTED,
      COMPLETED
    }

    Set<String> getNames();

    Type getType();
  }


  String getName();

  Map<String, RuntimeSpecification> getRunnables();

  /**
   * Returns a list of runnable names that should be executed in the given order.
   * @return
   */
  List<Order> getOrders();

  /**
   * Builder for constructing instance of {@link WeaveSpecification}.
   */
  public static final class Builder {

    private String name;
    private Map<String, RuntimeSpecification> runnables = Maps.newHashMap();
    private List<Order> orders = Lists.newArrayList();

    public static NameSetter with() {
      return new Builder().new NameSetter();
    }

    public final class NameSetter {
      public AfterName setName(String name) {
        Builder.this.name = name;
        return new AfterName();
      }
    }

    public final class AfterName {
      public MoreRunnable withRunnable() {
        return new RunnableSetter();
      }
    }

    public interface MoreRunnable {
      RuntimeSpecificationAdder add(WeaveRunnable runnable);

      RuntimeSpecificationAdder add(WeaveRunnable runnable, ResourceSpecification resourceSpec);

      /**
       * Adds a {@link WeaveRunnable} with {@link ResourceSpecification#BASIC} resource specification.
       * @param runnable
       * @return
       */
      RuntimeSpecificationAdder add(String name, WeaveRunnable runnable);

      RuntimeSpecificationAdder add(String name, WeaveRunnable runnable, ResourceSpecification resourceSpec);
    }

    public interface AfterRunnable {
      FirstOrder withOrder();

      AfterOrder anyOrder();
    }

    public final class RunnableSetter implements MoreRunnable, AfterRunnable {

      @Override
      public RuntimeSpecificationAdder add(WeaveRunnable runnable) {
        return add(runnable.configure().getName(), runnable);
      }

      @Override
      public RuntimeSpecificationAdder add(WeaveRunnable runnable, ResourceSpecification resourceSpec) {
        return add(runnable.configure().getName(), runnable, resourceSpec);
      }

      @Override
      public RuntimeSpecificationAdder add(String name, WeaveRunnable runnable) {
        return add(name, runnable, ResourceSpecification.BASIC);
      }

      @Override
      public RuntimeSpecificationAdder add(String name, WeaveRunnable runnable,
                                           final ResourceSpecification resourceSpec) {
        final WeaveRunnableSpecification spec = new DefaultWeaveRunnableSpecification(
                                            runnable.getClass().getName(), name, runnable.configure().getArguments());
        return new RuntimeSpecificationAdder(new Function<Collection<LocalFile>, RunnableSetter>() {
          @Override
          public RunnableSetter apply(Collection<LocalFile> files) {
            runnables.put(spec.getName(), new DefaultRuntimeSpecification(spec.getName(), spec, resourceSpec, files));
            return RunnableSetter.this;
          }
        });
      }

      @Override
      public FirstOrder withOrder() {
        return new OrderSetter();
      }

      @Override
      public AfterOrder anyOrder() {
        return new OrderSetter();
      }
    }

    /**
     * For setting runtime specific settings.
     */
    public final class RuntimeSpecificationAdder {

      private final Function<Collection<LocalFile>, RunnableSetter> completer;

      RuntimeSpecificationAdder(Function<Collection<LocalFile>, RunnableSetter> completer) {
        this.completer = completer;
      }

      public LocalFileAdder withLocalFiles() {
        return new MoreFile(completer);
      }

      public RunnableSetter noLocalFiles() {
        return completer.apply(ImmutableList.<LocalFile>of());
      }
    }

    public interface LocalFileAdder {
      MoreFile add(String name, File file);

      MoreFile add(String name, URI uri);

      MoreFile add(String name, File file, boolean archive);

      MoreFile add(String name, URI uri, boolean archive);

      MoreFile add(String name, File file, String pattern);

      MoreFile add(String name, URI uri, String pattern);
    }

    public final class MoreFile implements LocalFileAdder {

      private final Function<Collection<LocalFile>, RunnableSetter> completer;
      private final List<LocalFile> files = Lists.newArrayList();

      public MoreFile(Function<Collection<LocalFile>, RunnableSetter> completer) {
        this.completer = completer;
      }

      @Override
      public MoreFile add(String name, File file) {
        return add(name, file, false);
      }

      @Override
      public MoreFile add(String name, URI uri) {
        return add(name, uri, false);
      }

      @Override
      public MoreFile add(String name, File file, boolean archive) {
        return add(name, file.toURI(), archive);
      }

      @Override
      public MoreFile add(String name, URI uri, boolean archive) {
        files.add(new DefaultLocalFile(name, uri, archive, null));
        return this;
      }

      @Override
      public MoreFile add(String name, File file, String pattern) {
        return add(name, file.toURI(), pattern);
      }

      @Override
      public MoreFile add(String name, URI uri, String pattern) {
        files.add(new DefaultLocalFile(name, uri, true, pattern));
        return this;
      }

      public RunnableSetter apply() {
        return completer.apply(files);
      }
    }

    public interface FirstOrder {
      NextOrder begin(String name, String...names);
    }

    public interface NextOrder extends AfterOrder {
      NextOrder nextWhenStarted(String name, String...names);

      NextOrder nextWhenCompleted(String name, String...names);
    }

    public interface AfterOrder {
      WeaveSpecification build();
    }

    public final class OrderSetter implements FirstOrder, NextOrder {
      @Override
      public NextOrder begin(String name, String... names) {
        addOrder(Order.Type.STARTED, name, names);
        return this;
      }

      @Override
      public NextOrder nextWhenStarted(String name, String... names) {
        addOrder(Order.Type.STARTED, name, names);
        return this;
      }

      @Override
      public NextOrder nextWhenCompleted(String name, String... names) {
        addOrder(Order.Type.COMPLETED, name, names);
        return this;
      }

      @Override
      public WeaveSpecification build() {
        // Set to track with runnable hasn't been assigned an order.
        Set<String> runnableNames = Sets.newHashSet(runnables.keySet());
        for (Order order : orders) {
          runnableNames.removeAll(order.getNames());
        }

        // For all unordered runnables, add it to the end of orders list
        orders.add(new DefaultWeaveSpecification.DefaultOrder(runnableNames, Order.Type.STARTED));

        return new DefaultWeaveSpecification(name, runnables, orders);
      }

      private void addOrder(final Order.Type type, String name, String...names) {
        Preconditions.checkArgument(name != null, "Name cannot be null.");
        Preconditions.checkArgument(runnables.containsKey(name), "Runnable not exists.");

        Set<String> runnableNames = Sets.newHashSet(name);
        for (String runnableName : names) {
          Preconditions.checkArgument(name != null, "Name cannot be null.");
          Preconditions.checkArgument(runnables.containsKey(name), "Runnable not exists.");
          runnableNames.add(runnableName);
        }

        orders.add(new DefaultWeaveSpecification.DefaultOrder(runnableNames, type));
      }
    }

    private Builder() {}
  }
}
