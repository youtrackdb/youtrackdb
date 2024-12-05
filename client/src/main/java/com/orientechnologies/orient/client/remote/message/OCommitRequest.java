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

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class OCommitRequest implements OBinaryRequest<OCommitResponse> {

  private long txId;
  private boolean usingLong;
  private List<ORecordOperationRequest> operations;
  private EntityImpl indexChanges;

  public OCommitRequest() {
  }

  @Override
  public void write(YTDatabaseSessionInternal database, OChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    ORecordSerializer serializer = database.getSerializer();
    network.writeLong(txId);
    network.writeBoolean(usingLong);

    for (ORecordOperationRequest txEntry : operations) {
      network.writeByte((byte) 1);
      OMessageHelper.writeTransactionEntry(network, txEntry, serializer);
    }

    // END OF RECORD ENTRIES
    network.writeByte((byte) 0);

    // SEND MANUAL INDEX CHANGES
    network.writeBytes(indexChanges.toStream());
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput channel, int protocolVersion,
      ORecordSerializer serializer)
      throws IOException {
    txId = channel.readLong();
    usingLong = channel.readBoolean();
    operations = new ArrayList<>();

    byte hasEntry;
    do {
      hasEntry = channel.readByte();
      if (hasEntry == 1) {
        ORecordOperationRequest entry = OMessageHelper.readTransactionEntry(channel, serializer);
        operations.add(entry);
      }
    } while (hasEntry == 1);

    indexChanges = new EntityImpl(channel.readBytes());
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_TX_COMMIT;
  }

  @Override
  public String getDescription() {
    return "Transaction commit";
  }

  public EntityImpl getIndexChanges() {
    return indexChanges;
  }

  public List<ORecordOperationRequest> getOperations() {
    return operations;
  }

  public long getTxId() {
    return txId;
  }

  public boolean isUsingLong() {
    return usingLong;
  }

  @Override
  public OCommitResponse createResponse() {
    return new OCommitResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeCommit(this);
  }
}
