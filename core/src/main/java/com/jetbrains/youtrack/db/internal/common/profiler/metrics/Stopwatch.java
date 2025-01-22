package com.jetbrains.youtrack.db.internal.common.profiler.metrics;

/**
 * Special type of Gauge for time measurements.
 */
public interface Stopwatch extends Gauge<Double> {

  @FunctionalInterface
  interface XRunnable<X extends Throwable> {

    void run() throws X;
  }

  default <X extends Throwable> void timed(XRunnable<X> runnable) throws X {
    long start = System.nanoTime();
    try {
      runnable.run();
    } finally {
      setNanos(System.nanoTime() - start);
    }
  }

  default void setMillis(long millis) {
    setValue((double) millis);
  }

  default void setNanos(long nanos) {
    setValue((double) nanos / 1_000_000);
  }

  Stopwatch NOOP = new Stopwatch() {
    @Override
    public void setValue(Double value) {
      // do nothing
    }

    @Override
    public Double getValue() {
      return 0.0;
    }

    @Override
    public <X extends Throwable> void timed(XRunnable<X> runnable) throws X {
      runnable.run();
    }
  };

  static Stopwatch create() {
    return new Impl();
  }

  class Impl implements Stopwatch {

    private volatile Double value;

    public Double getValue() {
      return value;
    }

    public void setValue(Double value) {
      this.value = value;
    }
  }
}
