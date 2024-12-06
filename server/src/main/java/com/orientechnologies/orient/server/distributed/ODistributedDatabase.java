/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientechnologies.com)
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

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionId;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionSequenceStatus;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransacationMetadataHolder;
import com.jetbrains.youtrack.db.internal.core.tx.ValidationResult;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import java.util.List;
import java.util.Optional;

/**
 * Generic Distributed Database interface.
 */
public interface ODistributedDatabase {

  String getDatabaseName();

  void setOnline();

  String dump();

  void unlockResourcesOfServer(DatabaseSessionInternal database, String serverName);

  /**
   * Unlocks all the record locked by node iNodeName
   *
   * @param nodeName node id
   */
  void handleUnreachableNode(String nodeName);

  void waitForOnline();

  void reEnqueue(
      final int senderNodeId,
      final long msgSequence,
      final String databaseName,
      final ORemoteTask payload,
      int retryCount,
      int autoRetryDelay);

  void processRequest(ODistributedRequest request, boolean waitForAcceptingRequests);

  ValidationResult validate(FrontendTransactionId id);

  Optional<FrontendTransactionSequenceStatus> status();

  void rollback(FrontendTransactionId id);

  FrontendTransacationMetadataHolder commit(FrontendTransactionId id);

  ODistributedTxContext registerTxContext(
      final DistributedRequestId reqId, ODistributedTxContext ctx);

  ODistributedTxContext popTxContext(DistributedRequestId requestId);

  ODistributedTxContext getTxContext(DistributedRequestId requestId);

  ODistributedServerManager getManager();

  DatabaseSessionInternal getDatabaseInstance();

  long getReceivedRequests();

  long getProcessedRequests();

  Optional<FrontendTransactionId> nextId();

  List<FrontendTransactionId> missingTransactions(FrontendTransactionSequenceStatus lastState);

  void validateStatus(FrontendTransactionSequenceStatus status);

  void checkReverseSync(FrontendTransactionSequenceStatus lastState);

  void fillStatus();
}
