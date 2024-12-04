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
package com.orientechnologies.orient.core.fetch;

import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTFetchException;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;

/**
 *
 */
public interface OFetchContext {

  void onBeforeFetch(final YTDocument iRootRecord) throws YTFetchException;

  void onAfterFetch(final YTDocument iRootRecord) throws YTFetchException;

  void onBeforeArray(
      final YTDocument iRootRecord,
      final String iFieldName,
      final Object iUserObject,
      final YTIdentifiable[] iArray)
      throws YTFetchException;

  void onAfterArray(final YTDocument iRootRecord, final String iFieldName, final Object iUserObject)
      throws YTFetchException;

  void onBeforeCollection(
      final YTDocument iRootRecord,
      final String iFieldName,
      final Object iUserObject,
      final Iterable<?> iterable)
      throws YTFetchException;

  void onAfterCollection(
      final YTDocument iRootRecord, final String iFieldName, final Object iUserObject)
      throws YTFetchException;

  void onBeforeMap(final YTDocument iRootRecord, final String iFieldName, final Object iUserObject)
      throws YTFetchException;

  void onAfterMap(final YTDocument iRootRecord, final String iFieldName, final Object iUserObject)
      throws YTFetchException;

  void onBeforeDocument(
      final YTDocument iRecord,
      final YTDocument iDocument,
      final String iFieldName,
      final Object iUserObject)
      throws YTFetchException;

  void onAfterDocument(
      final YTDocument iRootRecord,
      final YTDocument iDocument,
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
