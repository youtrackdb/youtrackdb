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
import com.orientechnologies.orient.core.record.impl.YTDocument;

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

  public void onBeforeMap(YTDocument iRootRecord, String iFieldName, final Object iUserObject)
      throws YTFetchException {
  }

  public void onBeforeFetch(YTDocument iRootRecord) throws YTFetchException {
  }

  public void onBeforeArray(
      YTDocument iRootRecord, String iFieldName, Object iUserObject, YTIdentifiable[] iArray)
      throws YTFetchException {
  }

  public void onAfterArray(YTDocument iRootRecord, String iFieldName, Object iUserObject)
      throws YTFetchException {
  }

  public void onBeforeDocument(
      YTDocument iRecord, final YTDocument iDocument, String iFieldName, final Object iUserObject)
      throws YTFetchException {
  }

  public void onBeforeCollection(
      YTDocument iRootRecord,
      String iFieldName,
      final Object iUserObject,
      final Iterable<?> iterable)
      throws YTFetchException {
  }

  public void onAfterMap(YTDocument iRootRecord, String iFieldName, final Object iUserObject)
      throws YTFetchException {
  }

  public void onAfterFetch(YTDocument iRootRecord) throws YTFetchException {
  }

  public void onAfterDocument(
      YTDocument iRootRecord, final YTDocument iDocument, String iFieldName,
      final Object iUserObject)
      throws YTFetchException {
  }

  public void onAfterCollection(YTDocument iRootRecord, String iFieldName, final Object iUserObject)
      throws YTFetchException {
  }

  public boolean fetchEmbeddedDocuments() {
    return false;
  }
}
