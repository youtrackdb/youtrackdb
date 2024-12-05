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
package com.orientechnologies.orient.core.fetch.remote;

import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTFetchException;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;

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

  public void onBeforeMap(YTEntityImpl iRootRecord, String iFieldName, final Object iUserObject)
      throws YTFetchException {
  }

  public void onBeforeFetch(YTEntityImpl iRootRecord) throws YTFetchException {
  }

  public void onBeforeArray(
      YTEntityImpl iRootRecord, String iFieldName, Object iUserObject, YTIdentifiable[] iArray)
      throws YTFetchException {
  }

  public void onAfterArray(YTEntityImpl iRootRecord, String iFieldName, Object iUserObject)
      throws YTFetchException {
  }

  public void onBeforeDocument(
      YTEntityImpl iRecord, final YTEntityImpl iDocument, String iFieldName,
      final Object iUserObject)
      throws YTFetchException {
  }

  public void onBeforeCollection(
      YTEntityImpl iRootRecord,
      String iFieldName,
      final Object iUserObject,
      final Iterable<?> iterable)
      throws YTFetchException {
  }

  public void onAfterMap(YTEntityImpl iRootRecord, String iFieldName, final Object iUserObject)
      throws YTFetchException {
  }

  public void onAfterFetch(YTEntityImpl iRootRecord) throws YTFetchException {
  }

  public void onAfterDocument(
      YTEntityImpl iRootRecord, final YTEntityImpl iDocument, String iFieldName,
      final Object iUserObject)
      throws YTFetchException {
  }

  public void onAfterCollection(YTEntityImpl iRootRecord, String iFieldName,
      final Object iUserObject)
      throws YTFetchException {
  }

  public boolean fetchEmbeddedDocuments() {
    return false;
  }
}
