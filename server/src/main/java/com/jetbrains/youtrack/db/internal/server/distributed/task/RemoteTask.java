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
package com.jetbrains.youtrack.db.internal.server.distributed.task;

import com.jetbrains.youtrack.db.internal.core.command.CommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.server.YouTrackDBServer;
import com.jetbrains.youtrack.db.internal.server.distributed.DistributedRequest;
import com.jetbrains.youtrack.db.internal.server.distributed.DistributedRequestId;
import com.jetbrains.youtrack.db.internal.server.distributed.ODistributedDatabase;
import com.jetbrains.youtrack.db.internal.server.distributed.ODistributedServerManager;
import com.jetbrains.youtrack.db.internal.server.distributed.ORemoteTaskFactory;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Remote Task interface.
 */
public interface RemoteTask {

  boolean hasResponse();

  default void received(DistributedRequest request, ODistributedDatabase distributedDatabase) {
  }

  enum RESULT_STRATEGY {
    ANY,
    UNION
  }

  String getName();

  CommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType();

  Object execute(
      DistributedRequestId requestId,
      YouTrackDBServer iServer,
      ODistributedServerManager iManager,
      DatabaseSessionInternal database)
      throws Exception;

  long getDistributedTimeout();

  long getSynchronousTimeout(final int iSynchNodes);

  long getTotalTimeout(final int iTotalNodes);

  AbstractRemoteTask.RESULT_STRATEGY getResultStrategy();

  String getNodeSource();

  void setNodeSource(String nodeSource);

  boolean isIdempotent();

  boolean isNodeOnlineRequired();

  boolean isUsingDatabase();

  int getFactoryId();

  void toStream(DataOutput out) throws IOException;

  void fromStream(DataInput in, ORemoteTaskFactory factory) throws IOException;

  default void finished(ODistributedDatabase distributedDatabase) {
  }
}
