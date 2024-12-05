package com.orientechnologies.orient.server.distributed.task;

import com.jetbrains.youtrack.db.internal.common.concur.YTNeedRetryException;

public class YTDistributedKeyLockedException extends YTNeedRetryException {

  private String key;
  private String node;

  public YTDistributedKeyLockedException(final YTDistributedRecordLockedException exception) {
    super(exception);
  }

  public YTDistributedKeyLockedException(String localNodeName, Object key) {
    super("Cannot acquire lock on key '" + key + "' on server '" + localNodeName + "'.");
    this.key = key.toString();
    this.node = localNodeName;
  }

  public String getNode() {
    return node;
  }

  public String getKey() {
    return key;
  }
}
