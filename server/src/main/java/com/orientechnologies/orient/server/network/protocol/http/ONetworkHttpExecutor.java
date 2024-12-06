package com.orientechnologies.orient.server.network.protocol.http;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;

public interface ONetworkHttpExecutor {

  String getRemoteAddress();

  void setDatabase(DatabaseSessionInternal db);
}
