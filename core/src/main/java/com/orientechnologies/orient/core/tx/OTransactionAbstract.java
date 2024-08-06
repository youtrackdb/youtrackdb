/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;

public abstract class OTransactionAbstract implements OTransaction {
  protected ODatabaseDocumentInternal database;
  protected TXSTATUS status = TXSTATUS.INVALID;

  /**
   * Indicates the record deleted in a transaction.
   *
   * @see #getRecord(ORID)
   */
  public static final ORecordAbstract DELETED_RECORD = new ORecordBytes();

  protected OTransactionAbstract(final ODatabaseDocumentInternal iDatabase) {
    database = iDatabase;
  }

  public boolean isActive() {
    return status != TXSTATUS.INVALID
        && status != TXSTATUS.COMPLETED
        && status != TXSTATUS.ROLLED_BACK
        && status != TXSTATUS.ROLLBACKING;
  }

  public TXSTATUS getStatus() {
    return status;
  }

  public ODatabaseDocumentInternal getDatabase() {
    return database;
  }

  public abstract void internalRollback();

  public void setDatabase(ODatabaseDocumentInternal database) {
    this.database = database;
  }
}
