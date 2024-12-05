package com.jetbrains.youtrack.db.internal.common.util;

import com.jetbrains.youtrack.db.internal.common.log.OLogManager;

/**
 * Handler which is used to log all exceptions which are left uncaught by any exception handler.
 */
public class OUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

  @Override
  public void uncaughtException(Thread t, Throwable e) {
    final OLogManager logManager = OLogManager.instance();

    if (logManager != null) {
      OLogManager.instance().error(this, "Uncaught exception in thread %s", e, t.getName());
    }
  }
}
