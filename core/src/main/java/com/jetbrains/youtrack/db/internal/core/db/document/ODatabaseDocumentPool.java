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
package com.jetbrains.youtrack.db.internal.core.db.document;

import com.jetbrains.youtrack.db.internal.core.db.ODatabasePoolBase;
import com.jetbrains.youtrack.db.internal.core.db.PartitionedDatabasePool;
import com.jetbrains.youtrack.db.internal.core.db.PartitionedDatabasePoolFactory;

/**
 * @deprecated use {@link PartitionedDatabasePool} or
 * {@link PartitionedDatabasePoolFactory} instead.
 */
@Deprecated
public class ODatabaseDocumentPool extends ODatabasePoolBase {

  private static final ODatabaseDocumentPool globalInstance = new ODatabaseDocumentPool();

  public ODatabaseDocumentPool() {
    super();
  }

  public ODatabaseDocumentPool(
      final String iURL, final String iUserName, final String iUserPassword) {
    super(iURL, iUserName, iUserPassword);
  }

  public static ODatabaseDocumentPool global() {
    globalInstance.setup();
    return globalInstance;
  }

  public static ODatabaseDocumentPool global(final int iPoolMin, final int iPoolMax) {
    globalInstance.setup(iPoolMin, iPoolMax);
    return globalInstance;
  }

  @Override
  protected YTDatabaseDocumentTx createResource(
      Object owner, String iDatabaseName, Object... iAdditionalArgs) {
    return new YTDatabaseDocumentTxPooled(
        (ODatabaseDocumentPool) owner,
        iDatabaseName,
        (String) iAdditionalArgs[0],
        (String) iAdditionalArgs[1]);
  }
}
