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
package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.OInvalidIndexEngineIdException;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.storage.OStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.OAbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionIndexChangesPerKey;

/**
 * Dictionary index similar to unique index but does not check for updates, just executes changes.
 * Last put always wins and override the previous value.
 */
public class OIndexDictionary extends OIndexOneValue {

  public OIndexDictionary(OIndexMetadata im, final OStorage storage) {
    super(im, storage);
  }

  @Override
  public void doPut(YTDatabaseSessionInternal session, OAbstractPaginatedStorage storage,
      Object key,
      YTRID rid)
      throws OInvalidIndexEngineIdException {
    if (apiVersion == 0) {
      putV0(storage, indexId, key, rid);
    } else if (apiVersion == 1) {
      putV1(storage, indexId, key, rid);
    } else {
      throw new IllegalStateException("Invalid API version, " + apiVersion);
    }
  }

  @Override
  public boolean isNativeTxSupported() {
    return true;
  }

  private static void putV0(
      final OAbstractPaginatedStorage storage, int indexId, Object key, YTIdentifiable value)
      throws OInvalidIndexEngineIdException {
    throw new UnsupportedOperationException();
  }

  private static void putV1(
      final OAbstractPaginatedStorage storage, int indexId, Object key, YTIdentifiable value)
      throws OInvalidIndexEngineIdException {
    storage.putRidIndexEntry(indexId, key, value.getIdentity());
  }

  public boolean canBeUsedInEqualityOperators() {
    return true;
  }

  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public Iterable<OTransactionIndexChangesPerKey.OTransactionIndexEntry> interpretTxKeyChanges(
      OTransactionIndexChangesPerKey changes) {
    return changes.interpret(OTransactionIndexChangesPerKey.Interpretation.Dictionary);
  }
}
