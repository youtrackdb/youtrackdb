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

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OCollectionNetworkSerializer;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.BonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

public class OSBTGetRequest implements OBinaryRequest<OSBTGetResponse> {

  private BonsaiCollectionPointer collectionPointer;
  private byte[] keyStream;

  public OSBTGetRequest(BonsaiCollectionPointer collectionPointer, byte[] keyStream) {
    this.collectionPointer = collectionPointer;
    this.keyStream = keyStream;
  }

  public OSBTGetRequest() {
  }

  @Override
  public void write(DatabaseSessionInternal database, ChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(network, collectionPointer);
    network.writeBytes(keyStream);
  }

  public void read(DatabaseSessionInternal db, ChannelDataInput channel, int protocolVersion,
      RecordSerializer serializer)
      throws IOException {
    this.collectionPointer = OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(channel);
    this.keyStream = channel.readBytes();
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET;
  }

  @Override
  public String getDescription() {
    return "SB-Tree bonsai get";
  }

  public BonsaiCollectionPointer getCollectionPointer() {
    return collectionPointer;
  }

  public byte[] getKeyStream() {
    return keyStream;
  }

  @Override
  public OSBTGetResponse createResponse() {
    return new OSBTGetResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeSBTGet(this);
  }
}
