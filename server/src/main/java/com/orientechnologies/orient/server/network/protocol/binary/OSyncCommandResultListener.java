/*
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
 */

package com.orientechnologies.orient.server.network.protocol.binary;

import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.orientechnologies.orient.client.remote.OFetchPlanResults;
import com.jetbrains.youtrack.db.internal.core.command.OCommandResultListener;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTFetchException;
import com.jetbrains.youtrack.db.internal.core.fetch.OFetchContext;
import com.jetbrains.youtrack.db.internal.core.fetch.OFetchHelper;
import com.jetbrains.youtrack.db.internal.core.fetch.remote.ORemoteFetchContext;
import com.jetbrains.youtrack.db.internal.core.fetch.remote.ORemoteFetchListener;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import java.util.HashSet;
import java.util.Set;

/**
 * Synchronous command result manager.
 */
public class OSyncCommandResultListener extends OAbstractCommandResultListener
    implements OFetchPlanResults {

  private final Set<Record> fetchedRecordsToSend = new HashSet<Record>();
  private final Set<Record> alreadySent = new HashSet<Record>();

  public OSyncCommandResultListener(final OCommandResultListener wrappedResultListener) {
    super(wrappedResultListener);
  }

  @Override
  public boolean result(YTDatabaseSessionInternal querySession, final Object iRecord) {
    if (iRecord instanceof Record) {
      alreadySent.add((Record) iRecord);
      fetchedRecordsToSend.remove(iRecord);
    }

    if (wrappedResultListener != null)
    // NOTIFY THE WRAPPED LISTENER
    {
      wrappedResultListener.result(querySession, iRecord);
    }

    fetchRecord(
        iRecord,
        new ORemoteFetchListener() {
          @Override
          protected void sendRecord(RecordAbstract iLinked) {
            if (!alreadySent.contains(iLinked)) {
              fetchedRecordsToSend.add(iLinked);
            }
          }
        });
    return true;
  }

  public Set<Record> getFetchedRecordsToSend() {
    return fetchedRecordsToSend;
  }

  public boolean isEmpty() {
    return false;
  }

  @Override
  public void linkdedBySimpleValue(EntityImpl doc) {

    ORemoteFetchListener listener =
        new ORemoteFetchListener() {
          @Override
          protected void sendRecord(RecordAbstract iLinked) {
            if (!alreadySent.contains(iLinked)) {
              fetchedRecordsToSend.add(iLinked);
            }
          }

          @Override
          public void parseLinked(
              EntityImpl iRootRecord,
              YTIdentifiable iLinked,
              Object iUserObject,
              String iFieldName,
              OFetchContext iContext)
              throws YTFetchException {
            if (!(iLinked instanceof YTRecordId)) {
              sendRecord(iLinked.getRecord());
            }
          }

          @Override
          public void parseLinkedCollectionValue(
              EntityImpl iRootRecord,
              YTIdentifiable iLinked,
              Object iUserObject,
              String iFieldName,
              OFetchContext iContext)
              throws YTFetchException {

            if (!(iLinked instanceof YTRecordId)) {
              sendRecord(iLinked.getRecord());
            }
          }
        };
    final OFetchContext context = new ORemoteFetchContext();
    OFetchHelper.fetch(doc, doc, OFetchHelper.buildFetchPlan(""), listener, context, "");
  }
}
