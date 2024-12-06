/*
 *
 *  *  Copyright YouTrackDB
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
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionId;
import com.jetbrains.youtrack.db.internal.core.tx.TransactionInternal;
import java.util.Set;

/**
 * Represent a distributed transaction context.
 */
public interface ODistributedTxContext {

  FrontendTransactionId acquirePromise(RID rid, int version, boolean force);

  FrontendTransactionId acquireIndexKeyPromise(Object key, int version, boolean force);

  void releasePromises();

  DistributedRequestId getReqId();

  void commit(DatabaseSessionInternal database);

  Set<RecordId> rollback(DatabaseSessionInternal database);

  void destroy();

  void clearUndo();

  long getStartedOn();

  Set<RecordId> cancel(ODistributedServerManager current, DatabaseSessionInternal database);

  TransactionInternal getTransaction();

  FrontendTransactionId getTransactionId();

  void begin(DatabaseSessionInternal distributed, boolean local);
}
