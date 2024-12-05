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

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryAsyncRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OUpdateRecordRequest implements OBinaryAsyncRequest<OUpdateRecordResponse> {

  private YTRecordId rid;
  private byte[] rawContent;
  private RecordAbstract content;
  private int version;
  private boolean updateContent = true;
  private byte recordType;
  private byte mode;

  public OUpdateRecordRequest(
      YTRecordId iRid, byte[] iContent, int iVersion, boolean updateContent, byte iRecordType) {
    this.rid = iRid;
    this.rawContent = iContent;
    this.version = iVersion;
    this.updateContent = updateContent;
    this.recordType = iRecordType;
  }

  public OUpdateRecordRequest(
      YTRecordId iRid, RecordAbstract iContent, int iVersion, boolean updateContent,
      byte iRecordType) {
    this.rid = iRid;
    this.version = iVersion;
    this.updateContent = updateContent;
    this.content = iContent;
    this.recordType = iRecordType;
  }

  public OUpdateRecordRequest() {
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_RECORD_UPDATE;
  }

  @Override
  public String getDescription() {
    return "Update Record";
  }

  public void read(YTDatabaseSessionInternal db, OChannelDataInput channel, int protocolVersion,
      ORecordSerializer serializer)
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
        YouTrackDBManager.instance()
            .getRecordFactoryManager()
            .newInstance(recordType, rid, db);
    serializer.fromStream(db, bts, content, null);
  }

  @Override
  public void write(YTDatabaseSessionInternal database, final OChannelDataOutput network,
      final OStorageRemoteSession session)
      throws IOException {
    network.writeRID(rid);
    network.writeBoolean(updateContent);
    network.writeBytes(rawContent);
    network.writeVersion(version);
    network.writeByte(recordType);
    network.writeByte(mode);
  }

  public Record getContent() {
    return content;
  }

  public byte getMode() {
    return mode;
  }

  public byte getRecordType() {
    return recordType;
  }

  public YTRecordId getRid() {
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
  public OUpdateRecordResponse createResponse() {
    return new OUpdateRecordResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeUpdateRecord(this);
  }
}
