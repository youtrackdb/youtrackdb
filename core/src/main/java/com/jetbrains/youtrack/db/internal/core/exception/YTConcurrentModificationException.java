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
package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.concur.YTNeedRetryException;
import com.jetbrains.youtrack.db.internal.common.exception.OErrorCode;
import com.jetbrains.youtrack.db.internal.common.exception.YTHighLevelException;
import com.jetbrains.youtrack.db.internal.core.db.record.ORecordOperation;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import java.util.Locale;
import java.util.Objects;

/**
 * Exception thrown when MVCC is enabled and a record cannot be updated or deleted because versions
 * don't match.
 */
public class YTConcurrentModificationException extends YTNeedRetryException
    implements YTHighLevelException {

  private static final long serialVersionUID = 1L;

  private YTRID rid;
  private int databaseVersion = 0;
  private int recordVersion = 0;
  private int recordOperation;

  public YTConcurrentModificationException(YTConcurrentModificationException exception) {
    super(exception, OErrorCode.MVCC_ERROR);

    this.rid = exception.rid;
    this.recordVersion = exception.recordVersion;
    this.databaseVersion = exception.databaseVersion;
    this.recordOperation = exception.recordOperation;
  }

  protected YTConcurrentModificationException(final String message) {
    super(message);
  }

  public YTConcurrentModificationException(
      final YTRID iRID,
      final int iDatabaseVersion,
      final int iRecordVersion,
      final int iRecordOperation) {
    super(
        makeMessage(iRecordOperation, iRID, iDatabaseVersion, iRecordVersion),
        OErrorCode.MVCC_ERROR);

    if (YTFastConcurrentModificationException.enabled()) {
      throw new IllegalStateException(
          "Fast-throw is enabled. Use YTFastConcurrentModificationException.instance() instead");
    }

    rid = iRID;
    databaseVersion = iDatabaseVersion;
    recordVersion = iRecordVersion;
    recordOperation = iRecordOperation;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof YTConcurrentModificationException other)) {
      return false;
    }

    if (recordOperation == other.recordOperation && rid.equals(other.rid)) {
      if (databaseVersion == other.databaseVersion) {
        return recordOperation == other.recordOperation;
      }
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(rid, databaseVersion, recordVersion, recordOperation);
  }

  public int getEnhancedDatabaseVersion() {
    return databaseVersion;
  }

  public int getEnhancedRecordVersion() {
    return recordVersion;
  }

  public YTRID getRid() {
    return rid;
  }

  private static String makeMessage(
      int recordOperation, YTRID rid, int databaseVersion, int recordVersion) {
    final String operation = ORecordOperation.getName(recordOperation);

    String sb =
        "Cannot "
            + operation
            + " the record "
            + rid
            + " because the version is not the latest. Probably you are "
            + operation.toLowerCase(Locale.ENGLISH).substring(0, operation.length() - 1)
            + "ing an old record or it has been modified by another user (db=v"
            + databaseVersion
            + " your=v"
            + recordVersion
            + ")";
    return sb;
  }
}
