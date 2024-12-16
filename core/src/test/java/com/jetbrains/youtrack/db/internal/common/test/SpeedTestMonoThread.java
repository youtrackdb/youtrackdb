package com.jetbrains.youtrack.db.internal.common.test;

public abstract class SpeedTestMonoThread extends SpeedTestAbstract {

  protected SpeedTestMonoThread() {
  }

  protected SpeedTestMonoThread(final long iCycles) {
    super(iCycles);
  }
}
