package com.jetbrains.youtrack.db.internal.core.sql.executor;

/**
 *
 */
public interface OQueryMetrics {

  String getStatement();

  long getStartTime();

  long getElapsedTimeMillis();

  String getLanguage();
}
