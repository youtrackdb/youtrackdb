package com.jetbrains.youtrack.db.internal.server.distributed;

import com.jetbrains.youtrack.db.internal.server.distributed.task.RemoteTask;
import java.util.Collection;
import java.util.Set;

public interface ODistributedResponseManagerFactory {

  ODistributedResponseManager newResponseManager(
      DistributedRequest iRequest,
      Collection<String> iNodes,
      RemoteTask task,
      Set<String> nodesConcurToTheQuorum,
      int availableNodes,
      int expectedResponses,
      int quorum,
      boolean groupByResponse,
      boolean waitLocalNode);
}
