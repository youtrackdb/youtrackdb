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
import com.jetbrains.youtrack.db.internal.core.fetch.OFetchListener;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;

/**
 * Fetch listener for {@class ONetworkBinaryProtocol} class
 *
 * <p>Whenever a record has to be fetched it will be added to the list of records to send
 */
public abstract class ORemoteFetchListener implements OFetchListener {

  public boolean requireFieldProcessing() {
    return false;
  }

  public ORemoteFetchListener() {
  }

  protected abstract void sendRecord(RecordAbstract iLinked);

  public void processStandardField(
      EntityImpl iRecord,
      Object iFieldValue,
      String iFieldName,
      OFetchContext iContext,
      final Object iusObject,
      final String iFormat,
      YTType filedType)
      throws YTFetchException {
  }

  public void parseLinked(
      EntityImpl iRootRecord,
      YTIdentifiable iLinked,
      Object iUserObject,
      String iFieldName,
      OFetchContext iContext)
      throws YTFetchException {
  }

  public void parseLinkedCollectionValue(
      EntityImpl iRootRecord,
      YTIdentifiable iLinked,
      Object iUserObject,
      String iFieldName,
      OFetchContext iContext)
      throws YTFetchException {
  }

  public Object fetchLinkedMapEntry(
      EntityImpl iRoot,
      Object iUserObject,
      String iFieldName,
      String iKey,
      EntityImpl iLinked,
      OFetchContext iContext)
      throws YTFetchException {
    if (iLinked.getIdentity().isValid()) {
      sendRecord(iLinked);
      return true;
    }
    return null;
  }

  public Object fetchLinkedCollectionValue(
      EntityImpl iRoot,
      Object iUserObject,
      String iFieldName,
      EntityImpl iLinked,
      OFetchContext iContext)
      throws YTFetchException {
    if (iLinked.getIdentity().isValid()) {
      sendRecord(iLinked);
      return true;
    }
    return null;
  }

  public Object fetchLinked(
      EntityImpl iRoot,
      Object iUserObject,
      String iFieldName,
      EntityImpl iLinked,
      OFetchContext iContext)
      throws YTFetchException {
    sendRecord(iLinked);
    return true;
  }

  @Override
  public void skipStandardField(
      EntityImpl iRecord,
      String iFieldName,
      OFetchContext iContext,
      Object iUserObject,
      String iFormat)
      throws YTFetchException {
  }
}
