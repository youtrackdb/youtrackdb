package com.orientechnologies.core.db;

/**
 *
 */
public interface ODatabasePoolInternal extends AutoCloseable {

  YTDatabaseSession acquire();

  void close();

  void release(YTDatabaseSessionInternal database);

  YouTrackDBConfig getConfig();

  /**
   * Check if database pool is closed
   *
   * @return true if pool is closed
   */
  boolean isClosed();

  /**
   * Check that all resources owned by the pool are in the pool
   *
   * @return
   */
  boolean isUnused();

  /**
   * Check last time that a resource was returned to the pool
   *
   * @return
   */
  long getLastCloseTime();
}
