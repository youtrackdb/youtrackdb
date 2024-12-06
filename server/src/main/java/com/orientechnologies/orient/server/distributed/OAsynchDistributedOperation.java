/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.orientechnologies.orient.server.distributed;

import com.jetbrains.youtrack.db.internal.common.util.CallableFunction;
import com.jetbrains.youtrack.db.internal.common.util.Pair;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import java.util.Collection;
import java.util.Set;

/**
 * Asynchronous distributed operation.
 */
public class OAsynchDistributedOperation {

  private final String databaseName;
  private final Set<String> clusterNames;
  private final Collection<String> nodes;
  private final ORemoteTask task;
  private final long messageId;
  private final CallableFunction<Object, Pair<DistributedRequestId, Object>> callback;
  private final Object localResult;
  private final CallableFunction<Void, DistributedRequestId> afterRequestCallback;

  public OAsynchDistributedOperation(
      final String iDatabaseName,
      final Set<String> iClusterNames,
      final Collection<String> iNodes,
      final ORemoteTask iTask,
      final long iMessageId,
      final Object iLocalResult,
      final CallableFunction<Void, DistributedRequestId> iAfterRequestCallback,
      final CallableFunction<Object, Pair<DistributedRequestId, Object>> iCallback) {
    databaseName = iDatabaseName;
    clusterNames = iClusterNames;
    nodes = iNodes;
    task = iTask;
    messageId = iMessageId;
    callback = iCallback;
    localResult = iLocalResult;
    afterRequestCallback = iAfterRequestCallback;
  }

  public Set<String> getClusterNames() {
    return clusterNames;
  }

  public Collection<String> getNodes() {
    return nodes;
  }

  public ORemoteTask getTask() {
    return task;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public CallableFunction<Object, Pair<DistributedRequestId, Object>> getCallback() {
    return callback;
  }

  public Object getLocalResult() {
    return localResult;
  }

  public CallableFunction<Void, DistributedRequestId> getAfterSendCallback() {
    return afterRequestCallback;
  }

  public long getMessageId() {
    return messageId;
  }
}
