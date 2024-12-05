package com.orientechnologies.orient.server;

import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import java.io.IOException;

/**
 *
 */
public interface OServerAware {

  void init(OServer server);

  void coordinatedRequest(
      OClientConnection connection, int requestType, int clientTxId, OChannelBinary channel)
      throws IOException;

  ODistributedServerManager getDistributedManager();
}
