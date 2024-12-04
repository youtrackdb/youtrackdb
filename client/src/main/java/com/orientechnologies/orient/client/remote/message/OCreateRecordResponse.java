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

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class OCreateRecordResponse implements OBinaryResponse {

  private YTRecordId identity;
  private int version;
  private Map<UUID, OBonsaiCollectionPointer> changedIds;

  public OCreateRecordResponse() {
  }

  public OCreateRecordResponse(
      YTRecordId identity, int version, Map<UUID, OBonsaiCollectionPointer> changedIds) {
    this.identity = identity;
    this.version = version;
    this.changedIds = changedIds;
  }

  public void write(YTDatabaseSessionInternal session, OChannelDataOutput channel,
      int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeShort((short) this.identity.getClusterId());
    channel.writeLong(this.identity.getClusterPosition());
    channel.writeInt(version);
    if (protocolVersion >= 20) {
      OMessageHelper.writeCollectionChanges(channel, changedIds);
    }
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput network,
      OStorageRemoteSession session) throws IOException {
    short clusterId = network.readShort();
    long posistion = network.readLong();
    identity = new YTRecordId(clusterId, posistion);
    version = network.readVersion();
    changedIds = OMessageHelper.readCollectionChanges(network);
  }

  public YTRecordId getIdentity() {
    return identity;
  }

  public int getVersion() {
    return version;
  }

  public Map<UUID, OBonsaiCollectionPointer> getChangedIds() {
    return changedIds;
  }
}
