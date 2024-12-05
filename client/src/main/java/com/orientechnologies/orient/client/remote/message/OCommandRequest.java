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

import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import java.io.IOException;

public final class OCommandRequest implements OBinaryRequest<OCommandResponse> {

  private YTDatabaseSessionInternal database;
  private boolean asynch;
  private CommandRequestText query;
  private boolean live;

  public OCommandRequest(
      YTDatabaseSessionInternal database,
      boolean asynch,
      CommandRequestText iCommand,
      boolean live) {
    this.database = database;
    this.asynch = asynch;
    this.query = iCommand;
    this.live = live;
  }

  public OCommandRequest() {
  }

  @Override
  public void write(YTDatabaseSessionInternal database, OChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    if (live) {
      network.writeByte((byte) 'l');
    } else {
      network.writeByte((byte) (asynch ? 'a' : 's')); // ASYNC / SYNC
    }
    network.writeBytes(OStreamSerializerAnyStreamable.INSTANCE.toStream(query));
  }

  public void read(YTDatabaseSessionInternal db, OChannelDataInput channel, int protocolVersion,
      ORecordSerializer serializer)
      throws IOException {

    byte type = channel.readByte();
    if (type == (byte) 'l') {
      live = true;
    }
    if (type == (byte) 'a') {
      asynch = true;
    }
    query = OStreamSerializerAnyStreamable.INSTANCE.fromStream(db, channel.readBytes(), serializer);
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_COMMAND;
  }

  @Override
  public String getDescription() {
    return "Execute remote command";
  }

  public CommandRequestText getQuery() {
    return query;
  }

  public boolean isAsynch() {
    return asynch;
  }

  public boolean isLive() {
    return live;
  }

  @Override
  public OCommandResponse createResponse() {
    return new OCommandResponse(asynch, this.query.getResultListener(), database, live);
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeCommand(this);
  }
}
