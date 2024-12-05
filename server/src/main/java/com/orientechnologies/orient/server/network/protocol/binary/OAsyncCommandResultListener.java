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

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.command.OCommandResultListener;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTFetchException;
import com.jetbrains.youtrack.db.internal.core.fetch.OFetchContext;
import com.jetbrains.youtrack.db.internal.core.fetch.OFetchHelper;
import com.jetbrains.youtrack.db.internal.core.fetch.remote.ORemoteFetchContext;
import com.jetbrains.youtrack.db.internal.core.fetch.remote.ORemoteFetchListener;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.orientechnologies.orient.server.OClientConnection;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Asynchronous command result manager. As soon as a record is returned by the command is sent over
 * the wire.
 */
public class OAsyncCommandResultListener extends OAbstractCommandResultListener {

  private final ONetworkProtocolBinary protocol;
  private final AtomicBoolean empty = new AtomicBoolean(true);
  private final int txId;
  private final Set<YTRID> alreadySent = new HashSet<YTRID>();
  private final OClientConnection connection;

  public OAsyncCommandResultListener(
      OClientConnection connection, final OCommandResultListener wrappedResultListener) {
    super(wrappedResultListener);
    this.protocol = (ONetworkProtocolBinary) connection.getProtocol();
    this.txId = connection.getId();
    this.connection = connection;
  }

  @Override
  public boolean result(YTDatabaseSessionInternal querySession, final Object iRecord) {
    empty.compareAndSet(true, false);

    try {
      fetchRecord(
          iRecord,
          new ORemoteFetchListener() {
            @Override
            protected void sendRecord(RecordAbstract iLinked) {
              if (!alreadySent.contains(iLinked.getIdentity())) {
                alreadySent.add(iLinked.getIdentity());
                try {
                  protocol.channel.writeByte((byte) 2); // CACHE IT ON THE CLIENT
                  ONetworkProtocolBinary.writeIdentifiable(protocol.channel, connection, iLinked);
                } catch (IOException e) {
                  LogManager.instance().error(this, "Cannot write against channel", e);
                }
              }
            }
          });
      alreadySent.add(((YTIdentifiable) iRecord).getIdentity());
      protocol.channel.writeByte((byte) 1); // ONE MORE RECORD
      ONetworkProtocolBinary.writeIdentifiable(
          protocol.channel, connection, ((YTIdentifiable) iRecord).getRecord());
      protocol.channel.flush(); // TODO review this flush... it's for non blocking...

      if (wrappedResultListener != null)
      // NOTIFY THE WRAPPED LISTENER
      {
        wrappedResultListener.result(querySession, iRecord);
      }

    } catch (IOException e) {
      return false;
    }

    return true;
  }

  public boolean isEmpty() {
    return empty.get();
  }

  @Override
  public void linkdedBySimpleValue(EntityImpl doc) {
    ORemoteFetchListener listener =
        new ORemoteFetchListener() {
          @Override
          protected void sendRecord(RecordAbstract iLinked) {
            if (!alreadySent.contains(iLinked.getIdentity())) {
              alreadySent.add(iLinked.getIdentity());
              try {
                protocol.channel.writeByte((byte) 2); // CACHE IT ON THE CLIENT
                ONetworkProtocolBinary.writeIdentifiable(protocol.channel, connection, iLinked);
              } catch (IOException e) {
                LogManager.instance().error(this, "Cannot write against channel", e);
              }
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
            if (iLinked instanceof RecordAbstract record) {
              sendRecord(record);
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
            if (iLinked instanceof RecordAbstract record) {
              sendRecord(record);
            }
          }
        };
    final OFetchContext context = new ORemoteFetchContext();
    OFetchHelper.fetch(doc, doc, OFetchHelper.buildFetchPlan(""), listener, context, "");
  }
}
