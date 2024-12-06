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

import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OCollectionNetworkSerializer;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.Change;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.ChangeSerializationHelper;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.BonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;
import java.util.Map;

public class OSBTGetRealBagSizeRequest implements OBinaryRequest<OSBTGetRealBagSizeResponse> {

  private BonsaiCollectionPointer collectionPointer;
  private Map<Identifiable, Change> changes;
  private BinarySerializer<Identifiable> keySerializer;

  public OSBTGetRealBagSizeRequest() {
  }

  public OSBTGetRealBagSizeRequest(
      BinarySerializer<Identifiable> keySerializer,
      BonsaiCollectionPointer collectionPointer,
      Map<Identifiable, Change> changes) {
    this.collectionPointer = collectionPointer;
    this.changes = changes;
    this.keySerializer = keySerializer;
  }

  @Override
  public void write(DatabaseSessionInternal database, ChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(network, collectionPointer);
    final ChangeSerializationHelper changeSerializer = ChangeSerializationHelper.INSTANCE;
    final byte[] stream =
        new byte
            [IntegerSerializer.INT_SIZE
            + changeSerializer.getChangesSerializedSize(changes.size())];
    changeSerializer.serializeChanges(changes, keySerializer, stream, 0);
    network.writeBytes(stream);
  }

  public void read(DatabaseSessionInternal db, ChannelDataInput channel, int protocolVersion,
      RecordSerializer serializer)
      throws IOException {
    collectionPointer = OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(channel);
    byte[] stream = channel.readBytes();
    final ChangeSerializationHelper changeSerializer = ChangeSerializationHelper.INSTANCE;
    changes = changeSerializer.deserializeChanges(stream, 0);
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_RIDBAG_GET_SIZE;
  }

  @Override
  public String getDescription() {
    return "RidBag get size";
  }

  public Map<Identifiable, Change> getChanges() {
    return changes;
  }

  public BonsaiCollectionPointer getCollectionPointer() {
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
