package com.orientechnologies.orient.server;

import com.orientechnologies.orient.server.distributed.DistributedException;
import com.orientechnologies.orient.server.plugin.ServerPluginAbstract;

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
