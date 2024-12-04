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
package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OFetchPlanResults;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.client.remote.SimpleValueFetchPlanCommandListener;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.YTRecordAbstract;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37Client;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.core.sql.query.OBasicLegacyResultSet;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class OCommandResponse implements OBinaryResponse {

  private final boolean asynch;
  private final OCommandResultListener listener;
  private final YTDatabaseSessionInternal database;
  private boolean live;
  private Object result;
  private boolean isRecordResultSet;
  private OCommandRequestText command;
  private Map<Object, Object> params;

  public OCommandResponse(
      Object result,
      SimpleValueFetchPlanCommandListener listener,
      boolean isRecordResultSet,
      boolean async,
      YTDatabaseSessionInternal database,
      OCommandRequestText command,
      Map<Object, Object> params) {
    this.result = result;
    this.listener = listener;
    this.isRecordResultSet = isRecordResultSet;
    this.asynch = async;
    this.database = database;
    this.command = command;
    this.params = params;
  }

  public OCommandResponse(
      boolean asynch,
      OCommandResultListener listener,
      YTDatabaseSessionInternal database,
      boolean live) {
    this.asynch = asynch;
    this.listener = listener;
    this.database = database;
    this.live = live;
  }

  public void write(YTDatabaseSessionInternal session, OChannelDataOutput channel,
      int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    if (asynch) {
      if (params == null) {
        result = database.command(command).execute(session);
      } else {
        result = database.command(command).execute(session, params);
      }

      // FETCHPLAN HAS TO BE ASSIGNED AGAIN, because it can be changed by SQL statement
      channel.writeByte((byte) 0); // NO MORE RECORDS
    } else {
      serializeValue(
          database,
          channel,
          (SimpleValueFetchPlanCommandListener) listener,
          result,
          false,
          isRecordResultSet,
          protocolVersion,
          serializer);
      if (listener instanceof OFetchPlanResults) {
        // SEND FETCHED RECORDS TO LOAD IN CLIENT CACHE
        for (YTRecord rec : ((OFetchPlanResults) listener).getFetchedRecordsToSend()) {
          channel.writeByte((byte) 2); // CLIENT CACHE RECORD. IT
          // ISN'T PART OF THE
          // RESULT SET
          OMessageHelper.writeIdentifiable(session, channel, rec, serializer);
        }

        channel.writeByte((byte) 0); // NO MORE RECORDS
      }
    }
  }

  public void serializeValue(
      YTDatabaseSessionInternal session,
      OChannelDataOutput channel,
      final SimpleValueFetchPlanCommandListener listener,
      Object result,
      boolean load,
      boolean isRecordResultSet,
      int protocolVersion,
      ORecordSerializer recordSerializer)
      throws IOException {
    if (result == null) {
      // NULL VALUE
      channel.writeByte((byte) 'n');
    } else {
      if (result instanceof YTIdentifiable identifiable) {
        // RECORD
        channel.writeByte((byte) 'r');
        if (load && result instanceof YTRecordId) {
          result = ((YTRecordId) result).getRecord();
        }

        if (listener != null) {
          listener.result(session, result);
        }
        if (identifiable instanceof YTRecord record) {
          if (record.isNotBound(session)) {
            identifiable = session.bindToSession(record);
          }
        }

        OMessageHelper.writeIdentifiable(session, channel, identifiable, recordSerializer);
      } else {
        if (result instanceof ODocumentWrapper) {
          // RECORD
          channel.writeByte((byte) 'r');
          final YTDocument doc = ((ODocumentWrapper) result).getDocument(session);
          if (listener != null) {
            listener.result(session, doc);
          }
          OMessageHelper.writeIdentifiable(session, channel, doc, recordSerializer);
        } else {
          if (!isRecordResultSet) {
            writeSimpleValue(session, channel, listener, result, protocolVersion, recordSerializer);
          } else {
            if (OMultiValue.isMultiValue(result)) {
              final byte collectionType = result instanceof Set ? (byte) 's' : (byte) 'l';
              channel.writeByte(collectionType);
              channel.writeInt(OMultiValue.getSize(result));
              for (Object o : OMultiValue.getMultiValueIterable(result)) {
                try {
                  if (load && o instanceof YTRecordId) {
                    o = ((YTRecordId) o).getRecord();
                  }
                  if (listener != null) {
                    listener.result(session, o);
                  }

                  OMessageHelper.writeIdentifiable(session, channel, (YTIdentifiable) o,
                      recordSerializer);
                } catch (Exception e) {
                  OLogManager.instance().warn(this, "Cannot serialize record: " + o);
                  OMessageHelper.writeIdentifiable(session, channel, null, recordSerializer);
                  // WRITE NULL RECORD TO AVOID BREAKING PROTOCOL
                }
              }
            } else {
              if (OMultiValue.isIterable(result)) {
                if (protocolVersion >= OChannelBinaryProtocol.PROTOCOL_VERSION_32) {
                  channel.writeByte((byte) 'i');
                  for (Object o : OMultiValue.getMultiValueIterable(result)) {
                    try {
                      if (load && o instanceof YTRecordId) {
                        o = ((YTRecordId) o).getRecord();
                      }
                      if (listener != null) {
                        listener.result(session, o);
                      }

                      channel.writeByte((byte) 1); // ONE MORE RECORD
                      OMessageHelper.writeIdentifiable(session,
                          channel, (YTIdentifiable) o, recordSerializer);
                    } catch (Exception e) {
                      OLogManager.instance().warn(this, "Cannot serialize record: " + o);
                    }
                  }
                  channel.writeByte((byte) 0); // NO MORE RECORD
                } else {
                  // OLD RELEASES: TRANSFORM IN A COLLECTION
                  final byte collectionType = result instanceof Set ? (byte) 's' : (byte) 'l';
                  channel.writeByte(collectionType);
                  channel.writeInt(OMultiValue.getSize(result));
                  for (Object o : OMultiValue.getMultiValueIterable(result)) {
                    try {
                      if (load && o instanceof YTRecordId) {
                        o = ((YTRecordId) o).getRecord();
                      }
                      if (listener != null) {
                        listener.result(session, o);
                      }

                      OMessageHelper.writeIdentifiable(session,
                          channel, (YTIdentifiable) o, recordSerializer);
                    } catch (Exception e) {
                      OLogManager.instance().warn(this, "Cannot serialize record: " + o);
                    }
                  }
                }

              } else {
                // ANY OTHER (INCLUDING LITERALS)
                writeSimpleValue(session, channel, listener, result, protocolVersion,
                    recordSerializer);
              }
            }
          }
        }
      }
    }
  }

  private static void writeSimpleValue(
      YTDatabaseSessionInternal session, OChannelDataOutput channel,
      SimpleValueFetchPlanCommandListener listener,
      Object result,
      int protocolVersion,
      ORecordSerializer recordSerializer)
      throws IOException {

    if (protocolVersion >= OChannelBinaryProtocol.PROTOCOL_VERSION_35) {
      channel.writeByte((byte) 'w');
      YTDocument document = new YTDocument();
      document.field("result", result);
      OMessageHelper.writeIdentifiable(session, channel, document, recordSerializer);
      if (listener != null) {
        listener.linkdedBySimpleValue(document);
      }
    } else {
      channel.writeByte((byte) 'a');
      final StringBuilder value = new StringBuilder(64);
      if (listener != null) {
        YTDocument document = new YTDocument();
        document.field("result", result);
        listener.linkdedBySimpleValue(document);
      }
      ORecordSerializerStringAbstract.fieldTypeToString(
          value, YTType.getTypeByClass(result.getClass()), result);
      channel.writeString(value.toString());
    }
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput network,
      OStorageRemoteSession session) throws IOException {
    ORecordSerializer serializer = ORecordSerializerNetworkV37Client.INSTANCE;
    try {
      // Collection of prefetched temporary record (nested projection record), to refer for avoid
      // garbage collection.
      List<YTRecord> temporaryResults = new ArrayList<YTRecord>();

      boolean addNextRecord = true;
      if (asynch) {
        byte status;

        // ASYNCH: READ ONE RECORD AT TIME
        while ((status = network.readByte()) > 0) {
          final YTRecordAbstract record =
              (YTRecordAbstract) OMessageHelper.readIdentifiable(db, network, serializer);
          if (record == null) {
            continue;
          }

          switch (status) {
            case 1:
              // PUT AS PART OF THE RESULT SET. INVOKE THE LISTENER
              if (addNextRecord) {
                addNextRecord = listener.result(database, record);
                var cachedRecord = database.getLocalCache().findRecord(record.getIdentity());
                if (cachedRecord != record) {
                  if (cachedRecord != null) {
                    record.copyTo(cachedRecord);
                  } else {
                    database.getLocalCache().updateRecord(record);
                  }
                }
              }
              break;

            case 2:
              if (record.getIdentity().getClusterId() == -2) {
                temporaryResults.add(record);
              }
              var cachedRecord = database.getLocalCache().findRecord(record.getIdentity());
              if (cachedRecord != record) {
                if (cachedRecord != null) {
                  record.copyTo(cachedRecord);
                } else {
                  database.getLocalCache().updateRecord(record);
                }
              }
          }
        }
      } else {
        result = readSynchResult(network, database, temporaryResults);
        if (live) {
          final YTDocument doc = ((List<YTDocument>) result).get(0);
          final Integer token = doc.field("token");
          final Boolean unsubscribe = doc.field("unsubscribe");
          if (token != null) {
            //
            //            OStorageRemote storage = (OStorageRemote) database.getStorage();
            //            if (Boolean.TRUE.equals(unsubscribe)) {
            //              if (storage.asynchEventListener != null)
            //                storage.asynchEventListener.unregisterLiveListener(token);
            //            } else {
            //              final OLiveResultListener listener = (OLiveResultListener)
            // this.listener;
            //              final ODatabaseDocument dbCopy = database.copy();
            //              ORemoteConnectionPool pool =
            // storage.connectionManager.getPool(((OChannelBinaryAsynchClient)
            // network).getServerURL());
            //              storage.asynchEventListener.registerLiveListener(pool, token, new
            // OLiveResultListener() {
            //
            //                @Override
            //                public void onUnsubscribe(int iLiveToken) {
            //                  listener.onUnsubscribe(iLiveToken);
            //                  dbCopy.close();
            //                }
            //
            //                @Override
            //                public void onLiveResult(int iLiveToken, ORecordOperation iOp) throws
            // OException {
            //                  dbCopy.activateOnCurrentThread();
            //                  listener.onLiveResult(iLiveToken, iOp);
            //                }
            //
            //                @Override
            //                public void onError(int iLiveToken) {
            //                  listener.onError(iLiveToken);
            //                  dbCopy.close();
            //                }
            //              });
            //            }
          } else {
            throw new OStorageException("Cannot execute live query, returned null token");
          }
        }
      }
      if (!temporaryResults.isEmpty()) {
        if (result instanceof OBasicLegacyResultSet<?>) {
          ((OBasicLegacyResultSet<?>) result).setTemporaryRecordCache(temporaryResults);
        }
      }
    } finally {
      // TODO: this is here because we allow query in end listener.
      session.commandExecuting = false;
      if (listener != null && !live) {
        listener.end();
      }
    }
  }

  private Object readSynchResult(
      final OChannelDataInput network,
      final YTDatabaseSessionInternal database,
      List<YTRecord> temporaryResults)
      throws IOException {
    ORecordSerializer serializer = ORecordSerializerNetworkV37Client.INSTANCE;
    final Object result;

    final byte type = network.readByte();
    switch (type) {
      case 'n':
        result = null;
        break;

      case 'r':
        result = OMessageHelper.readIdentifiable(database, network, serializer);
        if (result instanceof YTRecordAbstract record) {
          var cachedRecord = database.getLocalCache().findRecord(record.getIdentity());
          if (cachedRecord != record) {
            if (cachedRecord != null) {
              record.copyTo(cachedRecord);
            } else {
              database.getLocalCache().updateRecord(record);
            }
          }
        }
        break;

      case 'l':
      case 's':
        final int tot = network.readInt();
        Collection<YTIdentifiable> coll =
            type == 's' ? new HashSet<>(tot) : new OBasicLegacyResultSet<>(tot);
        for (int i = 0; i < tot; ++i) {
          final YTIdentifiable resultItem = OMessageHelper.readIdentifiable(database, network,
              serializer);
          if (resultItem instanceof YTRecordAbstract record) {
            var rid = record.getIdentity();
            var cacheRecord = database.getLocalCache().findRecord(rid);

            if (cacheRecord != record) {
              if (cacheRecord != null) {
                record.copyTo(cacheRecord);
                coll.add(cacheRecord);
              } else {
                database.getLocalCache().updateRecord(record);
                coll.add(resultItem);
              }
            } else {
              coll.add(resultItem);
            }
          }
        }

        result = coll;
        break;
      case 'i':
        coll = new OBasicLegacyResultSet<YTIdentifiable>();
        byte status;
        while ((status = network.readByte()) > 0) {
          final YTIdentifiable record = OMessageHelper.readIdentifiable(database, network,
              serializer);
          if (record == null) {
            continue;
          }
          if (status == 1) {
            if (record instanceof YTRecordAbstract rec) {
              var cachedRecord = database.getLocalCache().findRecord(rec.getIdentity());

              if (cachedRecord != rec) {
                if (cachedRecord != null) {
                  rec.copyTo(cachedRecord);
                  coll.add(cachedRecord);
                } else {
                  database.getLocalCache().updateRecord(rec);
                  coll.add(rec);
                }
              } else {
                coll.add(rec);
              }
            }
          }
        }
        result = coll;
        break;
      case 'w':
        final YTIdentifiable record = OMessageHelper.readIdentifiable(database, network,
            serializer);
        // ((YTDocument) record).setLazyLoad(false);
        result = ((YTDocument) record).field("result");
        break;

      default:
        OLogManager.instance().warn(this, "Received unexpected result from query: %d", type);
        result = null;
    }

    // LOAD THE FETCHED RECORDS IN CACHE
    byte status;
    while ((status = network.readByte()) > 0) {
      YTRecordAbstract record =
          (YTRecordAbstract) OMessageHelper.readIdentifiable(database, network, serializer);
      if (record != null && status == 2) {
        // PUT IN THE CLIENT LOCAL CACHE
        var cachedRecord = database.getLocalCache().findRecord(record.getIdentity());
        if (cachedRecord != record) {
          if (cachedRecord != null) {
            record.copyTo(cachedRecord);
            record = cachedRecord;
          } else {
            database.getLocalCache().updateRecord(record);
          }
        }

        if (record.getIdentity().getClusterId() == -2) {
          temporaryResults.add(record);
        }
      }
    }

    return result;
  }

  public Object getResult() {
    return result;
  }
}
