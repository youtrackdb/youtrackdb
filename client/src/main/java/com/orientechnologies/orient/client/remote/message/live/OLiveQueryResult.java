package com.orientechnologies.orient.client.remote.message.live;

import com.orientechnologies.orient.core.sql.executor.YTResult;

/**
 *
 */
public class OLiveQueryResult {

  public static final byte CREATE_EVENT = 1;
  public static final byte UPDATE_EVENT = 2;
  public static final byte DELETE_EVENT = 3;

  private byte eventType;
  private YTResult currentValue;
  private YTResult oldValue;

  public OLiveQueryResult(byte eventType, YTResult currentValue, YTResult oldValue) {
    this.eventType = eventType;
    this.currentValue = currentValue;
    this.oldValue = oldValue;
  }

  public byte getEventType() {
    return eventType;
  }

  public void setOldValue(YTResult oldValue) {
    this.oldValue = oldValue;
  }

  public YTResult getCurrentValue() {
    return currentValue;
  }

  public void setCurrentValue(YTResult currentValue) {
    this.currentValue = currentValue;
  }

  public void setEventType(byte eventType) {
    this.eventType = eventType;
  }

  public YTResult getOldValue() {
    return oldValue;
  }
}
