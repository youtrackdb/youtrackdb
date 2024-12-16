package com.jetbrains.youtrack.db.internal.server.distributed;

public interface ORemoteServerAvailabilityCheck {

  boolean isNodeAvailable(String node);

  void nodeDisconnected(String node);
}
