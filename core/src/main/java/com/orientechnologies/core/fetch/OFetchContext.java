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
package com.orientechnologies.core.fetch;

import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.exception.YTFetchException;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;

/**
 *
 */
public interface OFetchContext {

  void onBeforeFetch(final YTEntityImpl iRootRecord) throws YTFetchException;

  void onAfterFetch(final YTEntityImpl iRootRecord) throws YTFetchException;

  void onBeforeArray(
      final YTEntityImpl iRootRecord,
      final String iFieldName,
      final Object iUserObject,
      final YTIdentifiable[] iArray)
      throws YTFetchException;

  void onAfterArray(final YTEntityImpl iRootRecord, final String iFieldName,
      final Object iUserObject)
      throws YTFetchException;

  void onBeforeCollection(
      final YTEntityImpl iRootRecord,
      final String iFieldName,
      final Object iUserObject,
      final Iterable<?> iterable)
      throws YTFetchException;

  void onAfterCollection(
      final YTEntityImpl iRootRecord, final String iFieldName, final Object iUserObject)
      throws YTFetchException;

  void onBeforeMap(final YTEntityImpl iRootRecord, final String iFieldName,
      final Object iUserObject)
      throws YTFetchException;

  void onAfterMap(final YTEntityImpl iRootRecord, final String iFieldName, final Object iUserObject)
      throws YTFetchException;

  void onBeforeDocument(
      final YTEntityImpl iRecord,
      final YTEntityImpl iDocument,
      final String iFieldName,
      final Object iUserObject)
      throws YTFetchException;

  void onAfterDocument(
      final YTEntityImpl iRootRecord,
      final YTEntityImpl iDocument,
      final String iFieldName,
      final Object iUserObject)
      throws YTFetchException;

  void onBeforeStandardField(
      final Object iFieldValue, final String iFieldName, final Object iUserObject,
      YTType fieldType);

  void onAfterStandardField(
      final Object iFieldValue, final String iFieldName, final Object iUserObject,
      YTType fieldType);

  boolean fetchEmbeddedDocuments();
}
