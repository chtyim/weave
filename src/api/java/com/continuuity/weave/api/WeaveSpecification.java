package com.continuuity.weave.api;

import com.continuuity.weave.internal.api.DefaultLocalFile;
import com.continuuity.weave.internal.api.DefaultRuntimeSpecification;
import com.continuuity.weave.internal.api.DefaultWeaveRunnableSpecification;
import com.continuuity.weave.internal.api.DefaultWeaveSpecification;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.File;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
public interface WeaveSpecification {

  String getName();

  Map<String, RuntimeSpecification> getRunnables();


  public static final class Builder {

    private String name;
    private Map<String, RuntimeSpecification> runnables = Maps.newHashMap();

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
      /**
       * Adds a {@link WeaveRunnable} with {@link ResourceSpecification#BASIC} resource specification.
       * @param runnable
       * @return
       */
      RuntimeSpecificationAdder add(WeaveRunnable runnable);

      RuntimeSpecificationAdder add(WeaveRunnable runnable, ResourceSpecification resourceSpec);
    }

    public interface AfterRunnable {
      WeaveSpecification build();
    }

    public final class RunnableSetter implements MoreRunnable, AfterRunnable {

      @Override
      public RuntimeSpecificationAdder add(WeaveRunnable runnable) {
        return add(runnable, ResourceSpecification.BASIC);
      }

      @Override
      public RuntimeSpecificationAdder add(WeaveRunnable runnable, final ResourceSpecification resourceSpec) {
        final WeaveRunnableSpecification spec = new DefaultWeaveRunnableSpecification(runnable.getClass().getName(),
                                                                                      runnable.configure());
        return new RuntimeSpecificationAdder(new Function<Collection<LocalFile>, RunnableSetter>() {
          @Override
          public RunnableSetter apply(Collection<LocalFile> files) {
            runnables.put(spec.getName(), new DefaultRuntimeSpecification(spec.getName(), spec, resourceSpec, files));
            return RunnableSetter.this;
          }
        });
      }

      @Override
      public WeaveSpecification build() {
        return new DefaultWeaveSpecification(name, runnables);
      }
    }

    public final class RuntimeSpecificationAdder {

      private final Function<Collection<LocalFile>, RunnableSetter> completer;

      public RuntimeSpecificationAdder(Function<Collection<LocalFile>, RunnableSetter> completer) {
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

    private Builder() {}
  }
}
