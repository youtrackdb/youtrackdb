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

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionId;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionInternal;
import java.util.Set;

/**
 * Represent a distributed transaction context.
 */
public interface ODistributedTxContext {

  OTransactionId acquirePromise(YTRID rid, int version, boolean force);

  OTransactionId acquireIndexKeyPromise(Object key, int version, boolean force);

  void releasePromises();

  ODistributedRequestId getReqId();

  void commit(YTDatabaseSessionInternal database);

  Set<YTRecordId> rollback(YTDatabaseSessionInternal database);

  void destroy();

  void clearUndo();

  long getStartedOn();

  Set<YTRecordId> cancel(ODistributedServerManager current, YTDatabaseSessionInternal database);

  OTransactionInternal getTransaction();

  OTransactionId getTransactionId();

  void begin(YTDatabaseSessionInternal distributed, boolean local);
}
