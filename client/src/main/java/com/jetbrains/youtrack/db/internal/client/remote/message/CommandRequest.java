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
import com.jetbrains.youtrack.db.internal.client.remote.db.DatabaseSessionRemote;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetwork;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.stream.StreamSerializerAnyStreamable;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

public final class CommandRequest implements BinaryRequest<CommandResponse> {

  private DatabaseSessionRemote database;
  private boolean asynch;
  private CommandRequestText query;
  private boolean live;

  public CommandRequest(
      DatabaseSessionRemote database,
      boolean asynch,
      CommandRequestText iCommand,
      boolean live) {
    this.database = database;
    this.asynch = asynch;
    this.query = iCommand;
    this.live = live;
  }

  public CommandRequest() {
  }

  @Override
  public void write(DatabaseSessionInternal databaseSession, ChannelDataOutput network,
      StorageRemoteSession session) throws IOException {
    if (live) {
      network.writeByte((byte) 'l');
    } else {
      network.writeByte((byte) (asynch ? 'a' : 's')); // ASYNC / SYNC
    }
    network.writeBytes(StreamSerializerAnyStreamable.toStream(databaseSession, query));
  }

  public void read(DatabaseSessionInternal databaseSession, ChannelDataInput channel,
      int protocolVersion,
      RecordSerializerNetwork serializer)
      throws IOException {

    var type = channel.readByte();
    if (type == (byte) 'l') {
      live = true;
    }
    if (type == (byte) 'a') {
      asynch = true;
    }
    query = StreamSerializerAnyStreamable.INSTANCE.fromStream(databaseSession, channel.readBytes(),
        serializer);
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_COMMAND;
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
  public CommandResponse createResponse() {
    return new CommandResponse(asynch, this.query.getResultListener(), database, live);
  }

  @Override
  public BinaryResponse execute(BinaryRequestExecutor executor) {
    return executor.executeCommand(this);
  }
}
