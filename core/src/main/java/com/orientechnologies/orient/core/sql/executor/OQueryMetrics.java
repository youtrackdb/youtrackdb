package com.orientechnologies.orient.core.sql.executor;

/**
 *
 */
public interface OQueryMetrics {

  String getStatement();

  long getStartTime();

  long getElapsedTimeMillis();

  String getLanguage();
}
