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

import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.client.binary.BinaryRequestExecutor;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryAsyncRequest;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetwork;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

public class CreateRecordRequest implements BinaryAsyncRequest<CreateRecordResponse> {

  private RecordAbstract content;
  private byte[] rawContent;
  private RecordId rid;
  private byte recordType;
  private byte mode;

  public CreateRecordRequest() {
  }

  public byte getMode() {
    return mode;
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_RECORD_CREATE;
  }

  @Override
  public String getDescription() {
    return "Create Record";
  }

  public CreateRecordRequest(byte[] iContent, RecordId iRid, byte iRecordType) {
    this.rawContent = iContent;
    this.rid = iRid;
    this.recordType = iRecordType;
  }

  public CreateRecordRequest(RecordAbstract iContent, RecordId iRid, byte iRecordType) {
    this.content = iContent;
    this.rid = iRid;
    this.recordType = iRecordType;
  }

  @Override
  public void write(DatabaseSessionInternal databaseSession, final ChannelDataOutput network,
      final StorageRemoteSession session)
      throws IOException {
    network.writeShort((short) rid.getClusterId());
    network.writeBytes(rawContent);
    network.writeByte(recordType);
    network.writeByte(mode);
  }

  public void read(DatabaseSessionInternal databaseSession, ChannelDataInput channel,
      int protocolVersion,
      RecordSerializerNetwork serializer)
      throws IOException {
    final var dataSegmentId = protocolVersion < 24 ? channel.readInt() : 0;

    rid = new RecordId(channel.readShort(), RID.CLUSTER_POS_INVALID);
    var rec = channel.readBytes();
    recordType = channel.readByte();
    mode = channel.readByte();
    content =
        YouTrackDBEnginesManager.instance()
            .getRecordFactoryManager()
            .newInstance(recordType, rid, databaseSession);
    serializer.fromStream(databaseSession, rec, content, null);
  }

  public RecordId getRid() {
    return rid;
  }

  public DBRecord getContent() {
    return content;
  }

  public byte getRecordType() {
    return recordType;
  }

  @Override
  public void setMode(byte mode) {
    this.mode = mode;
  }

  @Override
  public CreateRecordResponse createResponse() {
    return new CreateRecordResponse();
  }

  @Override
  public BinaryResponse execute(BinaryRequestExecutor executor) {
    return executor.executeCreateRecord(this);
  }
}
