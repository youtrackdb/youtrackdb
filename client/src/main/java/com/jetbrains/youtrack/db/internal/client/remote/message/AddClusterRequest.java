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

public final class AddClusterRequest implements BinaryRequest<AddClusterResponse> {

  private int requestedId = -1;
  private String clusterName;

  public AddClusterRequest(int iRequestedId, String iClusterName) {
    this.requestedId = iRequestedId;
    this.clusterName = iClusterName;
  }

  public AddClusterRequest() {
  }

  @Override
  public void write(DatabaseSessionInternal database, ChannelDataOutput network,
      StorageRemoteSession session) throws IOException {
    network.writeString(clusterName);
    network.writeShort((short) requestedId);
  }

  public void read(DatabaseSessionInternal db, ChannelDataInput channel, int protocolVersion,
      RecordSerializerNetwork serializer)
      throws IOException {
    String type = "";
    if (protocolVersion < 24) {
      type = channel.readString();
    }

    this.clusterName = channel.readString();

    if (protocolVersion < 24 || type.equalsIgnoreCase("PHYSICAL"))
    // Skipping location is just for compatibility
    {
      channel.readString();
    }

    if (protocolVersion < 24)
    // Skipping data segment name is just for compatibility
    {
      channel.readString();
    }

    this.requestedId = channel.readShort();
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_CLUSTER_ADD;
  }

  @Override
  public String getDescription() {
    return "Add cluster";
  }

  public String getClusterName() {
    return clusterName;
  }

  public int getRequestedId() {
    return requestedId;
  }

  @Override
  public AddClusterResponse createResponse() {
    return new AddClusterResponse();
  }

  @Override
  public BinaryResponse execute(BinaryRequestExecutor executor) {
    return executor.executeAddCluster(this);
  }
}
