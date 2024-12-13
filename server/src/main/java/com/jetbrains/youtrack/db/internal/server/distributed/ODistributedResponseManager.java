package com.jetbrains.youtrack.db.internal.server.distributed;

import java.util.List;
import java.util.Set;

public interface ODistributedResponseManager {

  boolean setLocalResult(String localNodeName, Object localResult);

  DistributedResponse getFinalResponse();

  void removeServerBecauseUnreachable(String node);

  boolean waitForSynchronousResponses() throws InterruptedException;

  long getSynchTimeout();

  void cancel();

  Set<String> getExpectedNodes();

  List<String> getRespondingNodes();

  DistributedRequestId getMessageId();

  int getQuorum();

  boolean collectResponse(DistributedResponse response);

  void timeout();

  long getSentOn();

  List<String> getMissingNodes();

  String getDatabaseName();

  boolean isFinished();
}
