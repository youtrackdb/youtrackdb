package com.orientechnologies.common.thread;

import com.orientechnologies.common.exception.YTException;

public class YTTracedExecutionException extends YTException {

  public YTTracedExecutionException(String message, Exception cause) {
    super(message);
    initCause(cause);
  }

  public YTTracedExecutionException(String message) {
    super(message);
  }

  private static String taskName(Object task) {
    if (task != null) {
      return task.getClass().getSimpleName();
    }
    return "?";
  }

  public static YTTracedExecutionException prepareTrace(Object task) {
    final YTTracedExecutionException trace;
    trace = new YTTracedExecutionException(String.format("Async task [%s] failed", taskName(task)));
    trace.fillInStackTrace();
    return trace;
  }

  public static YTTracedExecutionException trace(
      YTTracedExecutionException trace, Exception e, Object task)
      throws YTTracedExecutionException {
    if (trace != null) {
      trace.initCause(e);
      return trace;
    } else {
      return new YTTracedExecutionException(
          String.format("Async task [%s] failed", taskName(task)), e);
    }
  }
}
