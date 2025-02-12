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
import com.jetbrains.youtrack.db.internal.client.remote.BinaryAsyncRequest;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetwork;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

public class CleanOutRecordRequest implements BinaryAsyncRequest<CleanOutRecordResponse> {

  private int recordVersion;
  private RecordId recordId;
  private byte mode;

  public CleanOutRecordRequest() {
  }

  public CleanOutRecordRequest(int recordVersion, RecordId recordId) {
    this.recordVersion = recordVersion;
    this.recordId = recordId;
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_RECORD_CLEAN_OUT;
  }

  @Override
  public String getDescription() {
    return "Clean out record";
  }

  public void read(DatabaseSessionInternal databaseSession, ChannelDataInput channel,
      int protocolVersion,
      RecordSerializerNetwork serializer)
      throws IOException {
    recordId = channel.readRID();
    recordVersion = channel.readVersion();
    mode = channel.readByte();
  }

  @Override
  public void write(DatabaseSessionInternal databaseSession, ChannelDataOutput network,
      StorageRemoteSession session) throws IOException {
    network.writeRID(recordId);
    network.writeVersion(recordVersion);
    network.writeByte(mode);
  }

  public byte getMode() {
    return mode;
  }

  public RecordId getRecordId() {
    return recordId;
  }

  public int getRecordVersion() {
    return recordVersion;
  }

  public void setMode(byte mode) {
    this.mode = mode;
  }

  @Override
  public CleanOutRecordResponse createResponse() {
    return new CleanOutRecordResponse();
  }

  @Override
  public BinaryResponse execute(BinaryRequestExecutor executor) {
    return executor.executeCleanOutRecord(this);
  }
}
