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
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.BonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class OCreateRecordResponse implements OBinaryResponse {

  private RecordId identity;
  private int version;
  private Map<UUID, BonsaiCollectionPointer> changedIds;

  public OCreateRecordResponse() {
  }

  public OCreateRecordResponse(
      RecordId identity, int version, Map<UUID, BonsaiCollectionPointer> changedIds) {
    this.identity = identity;
    this.version = version;
    this.changedIds = changedIds;
  }

  public void write(DatabaseSessionInternal session, ChannelDataOutput channel,
      int protocolVersion, RecordSerializer serializer)
      throws IOException {
    channel.writeShort((short) this.identity.getClusterId());
    channel.writeLong(this.identity.getClusterPosition());
    channel.writeInt(version);
    if (protocolVersion >= 20) {
      OMessageHelper.writeCollectionChanges(channel, changedIds);
    }
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network,
      OStorageRemoteSession session) throws IOException {
    short clusterId = network.readShort();
    long posistion = network.readLong();
    identity = new RecordId(clusterId, posistion);
    version = network.readVersion();
    changedIds = OMessageHelper.readCollectionChanges(network);
  }

  public RecordId getIdentity() {
    return identity;
  }

  public int getVersion() {
    return version;
  }

  public Map<UUID, BonsaiCollectionPointer> getChangedIds() {
    return changedIds;
  }
}
