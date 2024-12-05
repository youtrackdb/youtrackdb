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
package com.orientechnologies.core.fetch;

import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.exception.YTFetchException;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;

/**
 * Listener interface used while fetching records.
 */
public interface OFetchListener {

  /**
   * Returns true if the listener fetches he fields.
   */
  boolean requireFieldProcessing();

  /**
   * Fetch the linked field.
   *
   * @param iRoot
   * @param iFieldName
   * @param iLinked
   * @return null if the fetching must stop, otherwise the current field value
   */
  Object fetchLinked(
      final YTEntityImpl iRoot,
      final Object iUserObject,
      final String iFieldName,
      final YTEntityImpl iLinked,
      final OFetchContext iContext)
      throws YTFetchException;

  void parseLinked(
      final YTEntityImpl iRootRecord,
      final YTIdentifiable iLinked,
      final Object iUserObject,
      final String iFieldName,
      final OFetchContext iContext)
      throws YTFetchException;

  void parseLinkedCollectionValue(
      final YTEntityImpl iRootRecord,
      final YTIdentifiable iLinked,
      final Object iUserObject,
      final String iFieldName,
      final OFetchContext iContext)
      throws YTFetchException;

  Object fetchLinkedMapEntry(
      final YTEntityImpl iRoot,
      final Object iUserObject,
      final String iFieldName,
      final String iKey,
      final YTEntityImpl iLinked,
      final OFetchContext iContext)
      throws YTFetchException;

  Object fetchLinkedCollectionValue(
      final YTEntityImpl iRoot,
      final Object iUserObject,
      final String iFieldName,
      final YTEntityImpl iLinked,
      final OFetchContext iContext)
      throws YTFetchException;

  void processStandardField(
      final YTEntityImpl iRecord,
      final Object iFieldValue,
      final String iFieldName,
      final OFetchContext iContext,
      final Object iUserObject,
      String iFormat,
      YTType filedType)
      throws YTFetchException;

  void skipStandardField(
      final YTEntityImpl iRecord,
      final String iFieldName,
      final OFetchContext iContext,
      final Object iUserObject,
      String iFormat)
      throws YTFetchException;
}
