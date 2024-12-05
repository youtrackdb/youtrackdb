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
package com.orientechnologies.core.db.document;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.core.db.ODatabasePoolBase;
import com.orientechnologies.core.db.ODatabasePooled;
import com.orientechnologies.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.core.metadata.security.OToken;

/**
 * Pooled wrapper to the YTDatabaseDocumentTx class. Allows to being reused across calls. The close()
 * method does not close the database for real but release it to the owner pool. The database born
 * as opened and will leave open until the pool is closed.
 *
 * @see ODatabasePoolBase
 */
@SuppressWarnings("unchecked")
public class YTDatabaseDocumentTxPooled extends YTDatabaseDocumentTx implements ODatabasePooled {

  private ODatabaseDocumentPool ownerPool;
  private final String userName;

  public YTDatabaseDocumentTxPooled(
      final ODatabaseDocumentPool iOwnerPool,
      final String iURL,
      final String iUserName,
      final String iUserPassword) {
    super(iURL);
    ownerPool = iOwnerPool;
    userName = iUserName;

    super.open(iUserName, iUserPassword);
  }

  public void reuse(final Object iOwner, final Object[] iAdditionalArgs) {
    ownerPool = (ODatabaseDocumentPool) iOwner;
    getLocalCache().invalidate();
    // getMetadata().reload();
    ODatabaseRecordThreadLocal.instance().set(this);

    try {
      callOnOpenListeners();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on reusing database '%s' in pool", e, getName());
    }
  }

  @Override
  public YTDatabaseSession open(final String iUserName, final String iUserPassword) {
    throw new UnsupportedOperationException(
        "Database instance was retrieved from a pool. You cannot open the database in this way. Use"
            + " directly a YTDatabaseDocumentTx instance if you want to manually open the"
            + " connection");
  }

  @Override
  public YTDatabaseSession open(final OToken iToken) {
    throw new UnsupportedOperationException(
        "Database instance was retrieved from a pool. You cannot open the database in this way. Use"
            + " directly a YTDatabaseDocumentTx instance if you want to manually open the"
            + " connection");
  }

  @Override
  public YTDatabaseSession create() {
    throw new UnsupportedOperationException(
        "Database instance was retrieved from a pool. You cannot open the database in this way. Use"
            + " directly a YTDatabaseDocumentTx instance if you want to manually open the"
            + " connection");
  }

  @Override
  public YTDatabaseSession create(String incrementalBackupPath) {
    throw new UnsupportedOperationException(
        "Database instance was retrieved from a pool. You cannot open the database in this way. Use"
            + " directly a YTDatabaseDocumentTx instance if you want to manually open the"
            + " connection");
  }

  public boolean isUnderlyingOpen() {
    return !super.isClosed();
  }

  @Override
  public boolean isClosed() {
    return ownerPool == null || super.isClosed();
  }

  /**
   * @return <code>true</code> if database is obtained from the pool and <code>false</code>
   * otherwise.
   */
  @Override
  public boolean isPooled() {
    return true;
  }

  /**
   * Avoid to close it but rather release itself to the owner pool.
   */
  @Override
  public void close() {
    if (isClosed()) {
      return;
    }

    checkOpenness();

    if (ownerPool != null && ownerPool.getConnectionsInCurrentThread(getURL(), userName) > 1) {
      ownerPool.release(this);
      return;
    }

    try {
      if (getTransaction().isActive()) {
        commit();
      }
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on releasing database '%s' in pool", e, getName());
    }

    try {
      callOnCloseListeners();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on releasing database '%s' in pool", e, getName());
    }

    getLocalCache().clear();

    if (ownerPool != null) {
      final ODatabaseDocumentPool localCopy = ownerPool;
      ownerPool = null;
      localCopy.release(this);
    }

    ODatabaseRecordThreadLocal.instance().remove();
  }

  public void forceClose() {
    super.close();
  }

  //  @Override
  protected void checkOpenness() {
    if (ownerPool == null) {
      throw new YTDatabaseException(
          "Database instance has been released to the pool. Get another database instance from the"
              + " pool with the right username and password");
    }

    //    super.checkOpenness();
  }
}
