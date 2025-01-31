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
package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.BonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import it.unimi.dsi.fastutil.objects.ObjectObjectImmutablePair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class Commit37Response implements BinaryResponse {

  private Map<UUID, BonsaiCollectionPointer> collectionChanges;
  private List<ObjectObjectImmutablePair<RecordId, RecordId>> updatedRids;

  public Commit37Response(
      Map<RecordId, RecordId> updatedRids, Map<UUID, BonsaiCollectionPointer> collectionChanges) {
    super();
    this.updatedRids = new ArrayList<>(updatedRids.size());

    for (var entry : updatedRids.entrySet()) {
      this.updatedRids.add(new ObjectObjectImmutablePair<>(entry.getValue(), entry.getKey()));
    }

    this.collectionChanges = collectionChanges;
  }

  public Commit37Response() {
  }

  @Override
  public void write(DatabaseSessionInternal session, ChannelDataOutput channel,
      int protocolVersion, RecordSerializer serializer)
      throws IOException {

    channel.writeInt(updatedRids.size());
    for (var pair : updatedRids) {
      channel.writeRID(pair.first());
      channel.writeRID(pair.second());
    }

    MessageHelper.writeCollectionChanges(channel, collectionChanges);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network,
      StorageRemoteSession session) throws IOException {

    var updatedRidsSize = network.readInt();
    updatedRids = new ArrayList<>(updatedRidsSize);
    for (var i = 0; i < updatedRidsSize; i++) {
      var first = network.readRID();
      var second = network.readRID();

      updatedRids.add(new ObjectObjectImmutablePair<>(first, second));
    }

    collectionChanges = MessageHelper.readCollectionChanges(network);
  }

  public List<ObjectObjectImmutablePair<RecordId, RecordId>> getUpdatedRids() {
    return updatedRids;
  }

  public Map<UUID, BonsaiCollectionPointer> getCollectionChanges() {
    return collectionChanges;
  }
}
