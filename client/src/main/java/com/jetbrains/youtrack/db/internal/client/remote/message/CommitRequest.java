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
import com.jetbrains.youtrack.db.internal.client.remote.message.tx.RecordOperationRequest;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class CommitRequest implements BinaryRequest<CommitResponse> {

  private long txId;
  private boolean usingLong;
  private List<RecordOperationRequest> operations;

  public CommitRequest() {
  }

  @Override
  public void write(DatabaseSessionInternal database, ChannelDataOutput network,
      StorageRemoteSession session) throws IOException {
    RecordSerializer serializer = database.getSerializer();
    network.writeLong(txId);
    network.writeBoolean(usingLong);

    for (RecordOperationRequest txEntry : operations) {
      network.writeByte((byte) 1);
      MessageHelper.writeTransactionEntry(network, txEntry, serializer);
    }

    // END OF RECORD ENTRIES
    network.writeByte((byte) 0);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput channel, int protocolVersion,
      RecordSerializer serializer)
      throws IOException {
    txId = channel.readLong();
    usingLong = channel.readBoolean();
    operations = new ArrayList<>();

    byte hasEntry;
    do {
      hasEntry = channel.readByte();
      if (hasEntry == 1) {
        RecordOperationRequest entry = MessageHelper.readTransactionEntry(channel, serializer);
        operations.add(entry);
      }
    } while (hasEntry == 1);
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_TX_COMMIT;
  }

  @Override
  public String getDescription() {
    return "Transaction commit";
  }

  public List<RecordOperationRequest> getOperations() {
    return operations;
  }

  public long getTxId() {
    return txId;
  }

  public boolean isUsingLong() {
    return usingLong;
  }

  @Override
  public CommitResponse createResponse() {
    return new CommitResponse();
  }

  @Override
  public BinaryResponse execute(BinaryRequestExecutor executor) {
    return executor.executeCommit(this);
  }
}
