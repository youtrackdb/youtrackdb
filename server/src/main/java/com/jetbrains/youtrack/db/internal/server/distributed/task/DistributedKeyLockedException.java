package com.jetbrains.youtrack.db.internal.server.distributed.task;

import com.jetbrains.youtrack.db.internal.common.concur.NeedRetryException;

public class DistributedKeyLockedException extends NeedRetryException {

  private String key;
  private String node;

  public DistributedKeyLockedException(final DistributedRecordLockedException exception) {
    super(exception);
  }

  public DistributedKeyLockedException(String localNodeName, Object key) {
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
