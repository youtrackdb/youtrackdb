package com.jetbrains.youtrack.db.internal.common.test;

public interface SpeedTest {

  void cycle() throws Exception;

  void init() throws Exception;

  void deinit() throws Exception;

  void beforeCycle() throws Exception;

  void afterCycle() throws Exception;
}
