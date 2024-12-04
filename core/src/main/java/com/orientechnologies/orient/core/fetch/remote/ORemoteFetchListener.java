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
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.fetch.OFetchListener;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTRecordAbstract;
import com.orientechnologies.orient.core.record.impl.YTDocument;

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

  protected abstract void sendRecord(YTRecordAbstract iLinked);

  public void processStandardField(
      YTDocument iRecord,
      Object iFieldValue,
      String iFieldName,
      OFetchContext iContext,
      final Object iusObject,
      final String iFormat,
      YTType filedType)
      throws OFetchException {
  }

  public void parseLinked(
      YTDocument iRootRecord,
      YTIdentifiable iLinked,
      Object iUserObject,
      String iFieldName,
      OFetchContext iContext)
      throws OFetchException {
  }

  public void parseLinkedCollectionValue(
      YTDocument iRootRecord,
      YTIdentifiable iLinked,
      Object iUserObject,
      String iFieldName,
      OFetchContext iContext)
      throws OFetchException {
  }

  public Object fetchLinkedMapEntry(
      YTDocument iRoot,
      Object iUserObject,
      String iFieldName,
      String iKey,
      YTDocument iLinked,
      OFetchContext iContext)
      throws OFetchException {
    if (iLinked.getIdentity().isValid()) {
      sendRecord(iLinked);
      return true;
    }
    return null;
  }

  public Object fetchLinkedCollectionValue(
      YTDocument iRoot,
      Object iUserObject,
      String iFieldName,
      YTDocument iLinked,
      OFetchContext iContext)
      throws OFetchException {
    if (iLinked.getIdentity().isValid()) {
      sendRecord(iLinked);
      return true;
    }
    return null;
  }

  public Object fetchLinked(
      YTDocument iRoot,
      Object iUserObject,
      String iFieldName,
      YTDocument iLinked,
      OFetchContext iContext)
      throws OFetchException {
    sendRecord(iLinked);
    return true;
  }

  @Override
  public void skipStandardField(
      YTDocument iRecord,
      String iFieldName,
      OFetchContext iContext,
      Object iUserObject,
      String iFormat)
      throws OFetchException {
  }
}
