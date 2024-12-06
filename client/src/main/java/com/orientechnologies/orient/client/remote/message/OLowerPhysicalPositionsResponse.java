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

import com.jetbrains.youtrack.db.internal.core.storage.PhysicalPosition;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

public class OLowerPhysicalPositionsResponse implements OBinaryResponse {

  private PhysicalPosition[] previousPositions;

  public OLowerPhysicalPositionsResponse() {
  }

  public OLowerPhysicalPositionsResponse(PhysicalPosition[] previousPositions) {
    this.previousPositions = previousPositions;
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network,
      OStorageRemoteSession session) throws IOException {
    this.previousPositions = OMessageHelper.readPhysicalPositions(network);
  }

  public void write(DatabaseSessionInternal session, ChannelDataOutput channel,
      int protocolVersion, RecordSerializer serializer)
      throws IOException {
    OMessageHelper.writePhysicalPositions(channel, previousPositions);
  }

  public PhysicalPosition[] getPreviousPositions() {
    return previousPositions;
  }
}
