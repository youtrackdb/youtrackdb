package com.jetbrains.youtrack.db.internal.client.remote.message.live;

import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;

/**
 *
 */
public class LiveQueryResult {

  public static final byte CREATE_EVENT = 1;
  public static final byte UPDATE_EVENT = 2;
  public static final byte DELETE_EVENT = 3;

  private byte eventType;
  private Result currentValue;
  private Result oldValue;

  public LiveQueryResult(byte eventType, Result currentValue, Result oldValue) {
    this.eventType = eventType;
    this.currentValue = currentValue;
    this.oldValue = oldValue;
  }

  public byte getEventType() {
    return eventType;
  }

  public void setOldValue(Result oldValue) {
    this.oldValue = oldValue;
  }

  public Result getCurrentValue() {
    return currentValue;
  }

  public void setCurrentValue(Result currentValue) {
    this.currentValue = currentValue;
  }

  public void setEventType(byte eventType) {
    this.eventType = eventType;
  }

  public Result getOldValue() {
    return oldValue;
  }
}
