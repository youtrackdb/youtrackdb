package com.jetbrains.youtrack.db.api.session;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.AcquireTimeoutException;

public interface SessionPool extends AutoCloseable {

  DatabaseSession acquire() throws AcquireTimeoutException;

  boolean isClosed();

  void close();
}
