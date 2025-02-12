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

import com.jetbrains.youtrack.db.internal.client.binary.BinaryRequestExecutor;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryRequest;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetwork;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

public final class CountRequest implements BinaryRequest<CountResponse> {

  private int[] clusterIds;
  private boolean countTombstones;

  public CountRequest(int[] iClusterIds, boolean countTombstones) {
    this.clusterIds = iClusterIds;
    this.countTombstones = countTombstones;
  }

  public CountRequest() {
  }

  @Override
  public void write(DatabaseSessionInternal databaseSession, ChannelDataOutput network,
      StorageRemoteSession session) throws IOException {
    network.writeShort((short) clusterIds.length);
    for (var iClusterId : clusterIds) {
      network.writeShort((short) iClusterId);
    }

    network.writeByte(countTombstones ? (byte) 1 : (byte) 0);
  }

  public void read(DatabaseSessionInternal databaseSession, ChannelDataInput channel,
      int protocolVersion,
      RecordSerializerNetwork serializer)
      throws IOException {
    int nclusters = channel.readShort();
    clusterIds = new int[nclusters];
    for (var i = 0; i < clusterIds.length; i++) {
      clusterIds[i] = channel.readShort();
    }
    countTombstones = channel.readByte() != 0;
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_CLUSTER_COUNT;
  }

  @Override
  public String getDescription() {
    return "Count cluster entities";
  }

  public int[] getClusterIds() {
    return clusterIds;
  }

  public boolean isCountTombstones() {
    return countTombstones;
  }

  @Override
  public CountResponse createResponse() {
    return new CountResponse();
  }

  @Override
  public BinaryResponse execute(BinaryRequestExecutor executor) {
    return executor.executeCountCluster(this);
  }
}
