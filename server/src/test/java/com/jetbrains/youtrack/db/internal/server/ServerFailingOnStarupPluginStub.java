package com.jetbrains.youtrack.db.internal.server;

import com.jetbrains.youtrack.db.internal.server.distributed.DistributedException;
import com.jetbrains.youtrack.db.internal.server.plugin.ServerPluginAbstract;

/**
 *
 */
public class ServerFailingOnStarupPluginStub extends ServerPluginAbstract {

  @Override
  public void startup() {
    throw new DistributedException("this plugin is not starting correctly");
  }

  @Override
  public String getName() {
    return "failing on startup plugin";
  }
}
