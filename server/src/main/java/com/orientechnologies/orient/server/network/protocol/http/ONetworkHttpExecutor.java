package com.orientechnologies.orient.server.network.protocol.http;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;

public interface ONetworkHttpExecutor {

  String getRemoteAddress();

  void setDatabase(ODatabaseSessionInternal db);
}
