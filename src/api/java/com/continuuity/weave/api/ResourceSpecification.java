package com.continuuity.weave.api;

/**
 *
 */
public interface ResourceSpecification {

  enum SizeUnit {
    MEGA(1),
    GIGA(1024);

    private final int multiplier;

    private SizeUnit(int multiplier) {
      this.multiplier = multiplier;
    }
  }

  /**
   * Returns the number of CPU cores.
   * @return Number of CPU cores.
   */
  int getCores();

  /**
   * Returns the memory size in MB.
   * @return Memory size
   */
  int getMemorySize();

  /**
   * Returns the uplink bandwidth in Mbps
   * @return Uplink bandwidth or -1 representing unlimited bandwidth.
   */
  int getUplink();

  /**
   * Returns the downlink bandwidth in Mbps
   * @return Downlink bandwidth or -1 representing unlimited bandwidth.
   */
  int getDownlink();

  /**
   * Builder for creating {@link ResourceSpecification}.
   */
  static final class Builder {

    private int cores;
    private int memory;
    private int uplink = -1;
    private int downlink = -1;

    public static CoreSetter with() {
      return new Builder().new CoreSetter();
    }

    public final class CoreSetter {
      public MemorySetter setCores(int cores) {
        Builder.this.cores = cores;
        return new MemorySetter();
      }
    }

    public final class MemorySetter {
      public AfterMemory setMemory(int size, SizeUnit unit) {
        Builder.this.memory = size * unit.multiplier;
        return new AfterMemory();
      }
    }

    public final class AfterMemory extends Build {
      public AfterUplink setUplink(int uplink, SizeUnit unit) {
        Builder.this.uplink = uplink * unit.multiplier;
        return new AfterUplink();
      }

      @Override
      public ResourceSpecification build() {
        // The override is just to make IDE shows better suggestion, as it thoughts this class define the build method.
        return super.build();
      }
    }

    public final class AfterUplink extends Build {
      public AfterDownlink setDownlink(int downlink, SizeUnit unit) {
        Builder.this.downlink = downlink * unit.multiplier;
        return new AfterDownlink();
      }

      @Override
      public ResourceSpecification build() {
        return super.build();
      }
    }

    public final class AfterDownlink extends Build {

      @Override
      public ResourceSpecification build() {
        return super.build();
      }
    }

    public abstract class Build {
      public ResourceSpecification build() {
        final int finalCores = cores;
        final int finalMemory = memory;
        final int finalUplink = uplink;
        final int finalDownlink = downlink;

        return new ResourceSpecification() {
          @Override
          public int getCores() {
            return finalCores;
          }

          @Override
          public int getMemorySize() {
            return finalMemory;
          }

          @Override
          public int getUplink() {
            return finalUplink;
          }

          @Override
          public int getDownlink() {
            return finalDownlink;
          }
        };
      }
    }

    private Builder() {}
  }
}
