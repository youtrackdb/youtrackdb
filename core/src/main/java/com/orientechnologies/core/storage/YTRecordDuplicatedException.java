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

package com.orientechnologies.core.storage;

import com.orientechnologies.common.exception.YTHighLevelException;
import com.orientechnologies.core.exception.YTCoreException;
import com.orientechnologies.core.id.YTRID;

/**
 * @since 9/5/12
 */
public class YTRecordDuplicatedException extends YTCoreException implements YTHighLevelException {

  private final YTRID rid;
  private final String indexName;
  private final Object key;

  public YTRecordDuplicatedException(final YTRecordDuplicatedException exception) {
    super(exception);
    this.indexName = exception.indexName;
    this.rid = exception.rid;
    this.key = exception.key;
  }

  public YTRecordDuplicatedException(
      final String message, final String indexName, final YTRID iRid, Object key) {
    super(message);
    this.indexName = indexName;
    this.rid = iRid;
    this.key = key;
  }

  public YTRID getRid() {
    return rid;
  }

  public String getIndexName() {
    return indexName;
  }

  public Object getKey() {
    return key;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null || !obj.getClass().equals(getClass())) {
      return false;
    }

    if (!indexName.equals(((YTRecordDuplicatedException) obj).indexName)) {
      return false;
    }

    return rid.equals(((YTRecordDuplicatedException) obj).rid);
  }

  @Override
  public int hashCode() {
    return rid.hashCode();
  }

  @Override
  public String toString() {
    return super.toString() + " INDEX=" + indexName + " RID=" + rid;
  }
}
