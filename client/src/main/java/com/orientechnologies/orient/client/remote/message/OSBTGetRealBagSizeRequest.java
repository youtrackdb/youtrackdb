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

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OCollectionNetworkSerializer;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.Change;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.ChangeSerializationHelper;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.Map;

public class OSBTGetRealBagSizeRequest implements OBinaryRequest<OSBTGetRealBagSizeResponse> {

  private OBonsaiCollectionPointer collectionPointer;
  private Map<YTIdentifiable, Change> changes;
  private OBinarySerializer<YTIdentifiable> keySerializer;

  public OSBTGetRealBagSizeRequest() {
  }

  public OSBTGetRealBagSizeRequest(
      OBinarySerializer<YTIdentifiable> keySerializer,
      OBonsaiCollectionPointer collectionPointer,
      Map<YTIdentifiable, Change> changes) {
    this.collectionPointer = collectionPointer;
    this.changes = changes;
    this.keySerializer = keySerializer;
  }

  @Override
  public void write(YTDatabaseSessionInternal database, OChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(network, collectionPointer);
    final ChangeSerializationHelper changeSerializer = ChangeSerializationHelper.INSTANCE;
    final byte[] stream =
        new byte
            [OIntegerSerializer.INT_SIZE
            + changeSerializer.getChangesSerializedSize(changes.size())];
    changeSerializer.serializeChanges(changes, keySerializer, stream, 0);
    network.writeBytes(stream);
  }

  public void read(YTDatabaseSessionInternal db, OChannelDataInput channel, int protocolVersion,
      ORecordSerializer serializer)
      throws IOException {
    collectionPointer = OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(channel);
    byte[] stream = channel.readBytes();
    final ChangeSerializationHelper changeSerializer = ChangeSerializationHelper.INSTANCE;
    changes = changeSerializer.deserializeChanges(stream, 0);
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_RIDBAG_GET_SIZE;
  }

  @Override
  public String getDescription() {
    return "RidBag get size";
  }

  public Map<YTIdentifiable, Change> getChanges() {
    return changes;
  }

  public OBonsaiCollectionPointer getCollectionPointer() {
    return collectionPointer;
  }

  @Override
  public OSBTGetRealBagSizeResponse createResponse() {
    return new OSBTGetRealBagSizeResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeSBTGetRealSize(this);
  }
}
