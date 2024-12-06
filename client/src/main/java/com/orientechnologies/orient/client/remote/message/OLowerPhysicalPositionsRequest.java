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
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.PhysicalPosition;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

public class OLowerPhysicalPositionsRequest
    implements OBinaryRequest<OLowerPhysicalPositionsResponse> {

  private PhysicalPosition physicalPosition;
  private int iClusterId;

  public OLowerPhysicalPositionsRequest(PhysicalPosition physicalPosition, int iClusterId) {
    this.physicalPosition = physicalPosition;
    this.iClusterId = iClusterId;
  }

  public OLowerPhysicalPositionsRequest() {
  }

  @Override
  public void write(DatabaseSessionInternal database, ChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    network.writeInt(iClusterId);
    network.writeLong(physicalPosition.clusterPosition);
  }

  public void read(DatabaseSessionInternal db, ChannelDataInput channel, int protocolVersion,
      RecordSerializer serializer)
      throws IOException {
    this.iClusterId = channel.readInt();
    this.physicalPosition = new PhysicalPosition(channel.readLong());
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_POSITIONS_LOWER;
  }

  @Override
  public String getDescription() {
    return "Retrieve lower positions";
  }

  public int getiClusterId() {
    return iClusterId;
  }

  public PhysicalPosition getPhysicalPosition() {
    return physicalPosition;
  }

  @Override
  public OLowerPhysicalPositionsResponse createResponse() {
    return new OLowerPhysicalPositionsResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeLowerPosition(this);
  }
}
