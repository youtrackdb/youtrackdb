package com.orientechnologies.orient.server.network.protocol.http;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;

public interface ONetworkHttpExecutor {

  String getRemoteAddress();

  void setDatabase(YTDatabaseSessionInternal db);
}
