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

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import it.unimi.dsi.fastutil.objects.ObjectObjectImmutablePair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class OCommitResponse implements OBinaryResponse {

  private Map<UUID, OBonsaiCollectionPointer> collectionChanges;
  private List<ObjectObjectImmutablePair<YTRID, YTRID>> updatedRids;

  public OCommitResponse(
      Map<YTRID, YTRID> updatedRids, Map<UUID, OBonsaiCollectionPointer> collectionChanges) {
    super();
    this.updatedRids = new ArrayList<>(updatedRids.size());

    for (Map.Entry<YTRID, YTRID> entry : updatedRids.entrySet()) {
      this.updatedRids.add(new ObjectObjectImmutablePair<>(entry.getValue(), entry.getKey()));
    }
    this.collectionChanges = collectionChanges;
  }

  public OCommitResponse() {
  }

  @Override
  public void write(YTDatabaseSessionInternal session, OChannelDataOutput channel,
      int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeInt(updatedRids.size());

    for (var pair : updatedRids) {
      channel.writeRID(pair.first());
      channel.writeRID(pair.second());
    }

    if (protocolVersion >= 20) {
      OMessageHelper.writeCollectionChanges(channel, collectionChanges);
    }
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput network,
      OStorageRemoteSession session) throws IOException {
    final int updatedRecordsCount = network.readInt();
    updatedRids = new ArrayList<>(updatedRecordsCount);

    for (int i = 0; i < updatedRecordsCount; i++) {
      var currentRid = network.readRID();
      var updated = network.readRID();

      updatedRids.add(new ObjectObjectImmutablePair<>(currentRid, updated));
    }

    collectionChanges = OMessageHelper.readCollectionChanges(network);
  }

  public List<ObjectObjectImmutablePair<YTRID, YTRID>> getUpdatedRids() {
    return updatedRids;
  }

  public Map<UUID, OBonsaiCollectionPointer> getCollectionChanges() {
    return collectionChanges;
  }
}
