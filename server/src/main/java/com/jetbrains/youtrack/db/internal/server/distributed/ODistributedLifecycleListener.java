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
package com.jetbrains.youtrack.db.internal.server.distributed;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import java.util.Set;

/**
 * Distributed lifecycle interface to catch event from the distributed cluster.
 */
public interface ODistributedLifecycleListener {

  /**
   * Called when a node is joining the cluster. Return false to deny the join.
   *
   * @param iNode Node name that is joining
   * @return true to allow the join, otherwise false
   */
  boolean onNodeJoining(String iNode);

  /**
   * Called right after a node joined the cluster.
   *
   * @param session
   * @param iNode   Node name that is joining
   */
  void onNodeJoined(DatabaseSessionInternal session, String iNode);

  /**
   * Called right after a node left the cluster.
   *
   * @param session
   * @param iNode   Node name that left
   */
  void onNodeLeft(DatabaseSessionInternal session, String iNode);

  /**
   * Called upon change of database status on a node. Available statuses are defined in
   * ODistributedServerManager.DB_STATUS.
   *
   * @param iNode         The node name
   * @param iDatabaseName Database name
   * @param iNewStatus    The new status
   * @since 2.2.0
   */
  void onDatabaseChangeStatus(
      String iNode, String iDatabaseName, ODistributedServerManager.DB_STATUS iNewStatus);

  default void onMessageReceived(DistributedRequest request) {
  }

  default void onMessagePartitionCalculated(
      DistributedRequest request, Set<Integer> involvedWorkerQueues) {
  }

  default void onMessageBeforeOp(String op, DistributedRequestId requestId) {
  }

  default void onMessageAfterOp(String op, DistributedRequestId requestId) {
  }

  default void onMessageProcessStart(DistributedRequest message) {
  }

  default void onMessageCurrentPayload(DistributedRequestId requestId, Object responsePayload) {
  }

  default void onMessageProcessEnd(DistributedRequest iRequest, Object responsePayload) {
  }
}
