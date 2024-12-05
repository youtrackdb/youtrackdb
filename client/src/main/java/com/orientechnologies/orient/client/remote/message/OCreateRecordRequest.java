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

import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryAsyncRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import java.io.IOException;

public class OCreateRecordRequest implements OBinaryAsyncRequest<OCreateRecordResponse> {

  private RecordAbstract content;
  private byte[] rawContent;
  private YTRecordId rid;
  private byte recordType;
  private byte mode;

  public OCreateRecordRequest() {
  }

  public byte getMode() {
    return mode;
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_RECORD_CREATE;
  }

  @Override
  public String getDescription() {
    return "Create Record";
  }

  public OCreateRecordRequest(byte[] iContent, YTRecordId iRid, byte iRecordType) {
    this.rawContent = iContent;
    this.rid = iRid;
    this.recordType = iRecordType;
  }

  public OCreateRecordRequest(RecordAbstract iContent, YTRecordId iRid, byte iRecordType) {
    this.content = iContent;
    this.rid = iRid;
    this.recordType = iRecordType;
  }

  @Override
  public void write(YTDatabaseSessionInternal database, final OChannelDataOutput network,
      final OStorageRemoteSession session)
      throws IOException {
    network.writeShort((short) rid.getClusterId());
    network.writeBytes(rawContent);
    network.writeByte(recordType);
    network.writeByte(mode);
  }

  public void read(YTDatabaseSessionInternal db, OChannelDataInput channel, int protocolVersion,
      ORecordSerializer serializer)
      throws IOException {
    final int dataSegmentId = protocolVersion < 24 ? channel.readInt() : 0;

    rid = new YTRecordId(channel.readShort(), YTRID.CLUSTER_POS_INVALID);
    byte[] rec = channel.readBytes();
    recordType = channel.readByte();
    mode = channel.readByte();
    content =
        YouTrackDBManager.instance()
            .getRecordFactoryManager()
            .newInstance(recordType, rid, db);
    serializer.fromStream(db, rec, content, null);
  }

  public YTRecordId getRid() {
    return rid;
  }

  public Record getContent() {
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
  public OCreateRecordResponse createResponse() {
    return new OCreateRecordResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeCreateRecord(this);
  }
}
