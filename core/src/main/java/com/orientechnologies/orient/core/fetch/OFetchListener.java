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
package com.orientechnologies.orient.core.fetch;

import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTFetchException;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;

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
      final YTDocument iRoot,
      final Object iUserObject,
      final String iFieldName,
      final YTDocument iLinked,
      final OFetchContext iContext)
      throws YTFetchException;

  void parseLinked(
      final YTDocument iRootRecord,
      final YTIdentifiable iLinked,
      final Object iUserObject,
      final String iFieldName,
      final OFetchContext iContext)
      throws YTFetchException;

  void parseLinkedCollectionValue(
      final YTDocument iRootRecord,
      final YTIdentifiable iLinked,
      final Object iUserObject,
      final String iFieldName,
      final OFetchContext iContext)
      throws YTFetchException;

  Object fetchLinkedMapEntry(
      final YTDocument iRoot,
      final Object iUserObject,
      final String iFieldName,
      final String iKey,
      final YTDocument iLinked,
      final OFetchContext iContext)
      throws YTFetchException;

  Object fetchLinkedCollectionValue(
      final YTDocument iRoot,
      final Object iUserObject,
      final String iFieldName,
      final YTDocument iLinked,
      final OFetchContext iContext)
      throws YTFetchException;

  void processStandardField(
      final YTDocument iRecord,
      final Object iFieldValue,
      final String iFieldName,
      final OFetchContext iContext,
      final Object iUserObject,
      String iFormat,
      YTType filedType)
      throws YTFetchException;

  void skipStandardField(
      final YTDocument iRecord,
      final String iFieldName,
      final OFetchContext iContext,
      final Object iUserObject,
      String iFormat)
      throws YTFetchException;
}
