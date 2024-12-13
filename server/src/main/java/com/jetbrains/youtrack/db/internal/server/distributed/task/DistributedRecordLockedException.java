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

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.common.concur.NeedRetryException;
import com.jetbrains.youtrack.db.internal.server.distributed.DistributedRequestId;

/**
 * Exception thrown when a record is locked by a running distributed transaction.
 */
public class DistributedRecordLockedException extends NeedRetryException {

  protected RID rid;
  protected DistributedRequestId lockHolder;
  protected String node;

  public DistributedRecordLockedException(final DistributedRecordLockedException exception) {
    super(exception);
  }

  public DistributedRecordLockedException(final String localNodeName, final RID iRid) {
    super("Cannot acquire lock on record " + iRid + " on server '" + localNodeName + "'. ");
    this.rid = iRid;
    this.node = localNodeName;
  }

  public DistributedRecordLockedException(
      final String localNodeName,
      final RID iRid,
      final DistributedRequestId iLockingRequestId,
      long timeout) {
    super(
        "Timeout ("
            + timeout
            + "ms) on acquiring lock on record "
            + iRid
            + " on server '"
            + localNodeName
            + "'. It is locked by request "
            + iLockingRequestId);
    this.rid = iRid;
    this.lockHolder = iLockingRequestId;
    this.node = localNodeName;
  }

  public RID getRid() {
    return rid;
  }

  public String getNode() {
    return node;
  }

  public DistributedRequestId getLockHolder() {
    return lockHolder;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof DistributedRecordLockedException)) {
      return false;
    }

    return rid.equals(((DistributedRecordLockedException) obj).rid);
  }
}
