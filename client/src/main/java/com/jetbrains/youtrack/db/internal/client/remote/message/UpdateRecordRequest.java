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
import com.jetbrains.youtrack.db.internal.client.binary.BinaryRequestExecutor;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryAsyncRequest;
import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

public class UpdateRecordRequest implements BinaryAsyncRequest<UpdateRecordResponse> {

  private RecordId rid;
  private byte[] rawContent;
  private RecordAbstract content;
  private int version;
  private boolean updateContent = true;
  private byte recordType;
  private byte mode;

  public UpdateRecordRequest(
      RecordId iRid, byte[] iContent, int iVersion, boolean updateContent, byte iRecordType) {
    this.rid = iRid;
    this.rawContent = iContent;
    this.version = iVersion;
    this.updateContent = updateContent;
    this.recordType = iRecordType;
  }

  public UpdateRecordRequest(
      RecordId iRid, RecordAbstract iContent, int iVersion, boolean updateContent,
      byte iRecordType) {
    this.rid = iRid;
    this.version = iVersion;
    this.updateContent = updateContent;
    this.content = iContent;
    this.recordType = iRecordType;
  }

  public UpdateRecordRequest() {
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_RECORD_UPDATE;
  }

  @Override
  public String getDescription() {
    return "Update Record";
  }

  public void read(DatabaseSessionInternal db, ChannelDataInput channel, int protocolVersion,
      RecordSerializer serializer)
      throws IOException {
    rid = channel.readRID();
    if (protocolVersion >= 23) {
      updateContent = channel.readBoolean();
    }
    byte[] bts = channel.readBytes();
    version = channel.readVersion();
    recordType = channel.readByte();
    mode = channel.readByte();

    content =
        YouTrackDBEnginesManager.instance()
            .getRecordFactoryManager()
            .newInstance(recordType, rid, db);
    serializer.fromStream(db, bts, content, null);
  }

  @Override
  public void write(DatabaseSessionInternal database, final ChannelDataOutput network,
      final StorageRemoteSession session)
      throws IOException {
    network.writeRID(rid);
    network.writeBoolean(updateContent);
    network.writeBytes(rawContent);
    network.writeVersion(version);
    network.writeByte(recordType);
    network.writeByte(mode);
  }

  public DBRecord getContent() {
    return content;
  }

  public byte getMode() {
    return mode;
  }

  public byte getRecordType() {
    return recordType;
  }

  public RecordId getRid() {
    return rid;
  }

  public int getVersion() {
    return version;
  }

  public boolean isUpdateContent() {
    return updateContent;
  }

  public void setMode(byte mode) {
    this.mode = mode;
  }

  @Override
  public UpdateRecordResponse createResponse() {
    return new UpdateRecordResponse();
  }

  @Override
  public BinaryResponse execute(BinaryRequestExecutor executor) {
    return executor.executeUpdateRecord(this);
  }
}
