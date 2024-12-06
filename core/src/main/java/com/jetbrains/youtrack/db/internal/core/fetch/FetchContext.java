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
package com.jetbrains.youtrack.db.internal.core.fetch;

import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.exception.FetchException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;

/**
 *
 */
public interface FetchContext {

  void onBeforeFetch(final EntityImpl iRootRecord) throws FetchException;

  void onAfterFetch(final EntityImpl iRootRecord) throws FetchException;

  void onBeforeArray(
      final EntityImpl iRootRecord,
      final String iFieldName,
      final Object iUserObject,
      final Identifiable[] iArray)
      throws FetchException;

  void onAfterArray(final EntityImpl iRootRecord, final String iFieldName,
      final Object iUserObject)
      throws FetchException;

  void onBeforeCollection(
      final EntityImpl iRootRecord,
      final String iFieldName,
      final Object iUserObject,
      final Iterable<?> iterable)
      throws FetchException;

  void onAfterCollection(
      final EntityImpl iRootRecord, final String iFieldName, final Object iUserObject)
      throws FetchException;

  void onBeforeMap(final EntityImpl iRootRecord, final String iFieldName,
      final Object iUserObject)
      throws FetchException;

  void onAfterMap(final EntityImpl iRootRecord, final String iFieldName, final Object iUserObject)
      throws FetchException;

  void onBeforeDocument(
      final EntityImpl iRecord,
      final EntityImpl entity,
      final String iFieldName,
      final Object iUserObject)
      throws FetchException;

  void onAfterDocument(
      final EntityImpl iRootRecord,
      final EntityImpl entity,
      final String iFieldName,
      final Object iUserObject)
      throws FetchException;

  void onBeforeStandardField(
      final Object iFieldValue, final String iFieldName, final Object iUserObject,
      PropertyType fieldType);

  void onAfterStandardField(
      final Object iFieldValue, final String iFieldName, final Object iUserObject,
      PropertyType fieldType);

  boolean fetchEmbeddedDocuments();
}
