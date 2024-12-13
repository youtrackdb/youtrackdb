package com.jetbrains.youtrack.db.internal.server;

import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.SocketChannelBinary;
import com.jetbrains.youtrack.db.internal.server.distributed.ODistributedServerManager;
import java.io.IOException;

/**
 *
 */
public interface OServerAware {

  void init(YouTrackDBServer server);

  void coordinatedRequest(
      ClientConnection connection, int requestType, int clientTxId, SocketChannelBinary channel)
      throws IOException;

  ODistributedServerManager getDistributedManager();
}
