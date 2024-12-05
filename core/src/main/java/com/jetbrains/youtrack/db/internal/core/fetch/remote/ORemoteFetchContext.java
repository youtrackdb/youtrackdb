/*
 *
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jetbrains.youtrack.db.internal.core.fetch.remote;

import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTFetchException;
import com.jetbrains.youtrack.db.internal.core.fetch.OFetchContext;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;

/**
 * Fetch context for {@class ONetworkBinaryProtocol} class
 */
public class ORemoteFetchContext implements OFetchContext {

  public void onBeforeStandardField(
      Object iFieldValue, String iFieldName, Object iUserObject, YTType fieldType) {
  }

  public void onAfterStandardField(
      Object iFieldValue, String iFieldName, Object iUserObject, YTType fieldType) {
  }

  public void onBeforeMap(EntityImpl iRootRecord, String iFieldName, final Object iUserObject)
      throws YTFetchException {
  }

  public void onBeforeFetch(EntityImpl iRootRecord) throws YTFetchException {
  }

  public void onBeforeArray(
      EntityImpl iRootRecord, String iFieldName, Object iUserObject, YTIdentifiable[] iArray)
      throws YTFetchException {
  }

  public void onAfterArray(EntityImpl iRootRecord, String iFieldName, Object iUserObject)
      throws YTFetchException {
  }

  public void onBeforeDocument(
      EntityImpl iRecord, final EntityImpl iDocument, String iFieldName,
      final Object iUserObject)
      throws YTFetchException {
  }

  public void onBeforeCollection(
      EntityImpl iRootRecord,
      String iFieldName,
      final Object iUserObject,
      final Iterable<?> iterable)
      throws YTFetchException {
  }

  public void onAfterMap(EntityImpl iRootRecord, String iFieldName, final Object iUserObject)
      throws YTFetchException {
  }

  public void onAfterFetch(EntityImpl iRootRecord) throws YTFetchException {
  }

  public void onAfterDocument(
      EntityImpl iRootRecord, final EntityImpl iDocument, String iFieldName,
      final Object iUserObject)
      throws YTFetchException {
  }

  public void onAfterCollection(EntityImpl iRootRecord, String iFieldName,
      final Object iUserObject)
      throws YTFetchException {
  }

  public boolean fetchEmbeddedDocuments() {
    return false;
  }
}
