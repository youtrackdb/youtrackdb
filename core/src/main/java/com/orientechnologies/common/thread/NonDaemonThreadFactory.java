package com.orientechnologies.common.thread;

import com.orientechnologies.common.util.OUncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class NonDaemonThreadFactory implements ThreadFactory {

  private final AtomicLong counter = new AtomicLong();
  private final String prefix;

  public NonDaemonThreadFactory(String prefix) {
    this.prefix = prefix;
  }

  @Override
  public Thread newThread(Runnable r) {
    final Thread thread = new Thread(r, prefix + " #" + counter.incrementAndGet());
    thread.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());

    return thread;
  }
}
