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

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.record.YTRecordAbstract;
import com.orientechnologies.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37Client;
import com.orientechnologies.core.storage.ORawBuffer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.Set;

public final class OReadRecordResponse implements OBinaryResponse {

  private byte recordType;
  private int version;
  private byte[] record;
  private Set<YTRecordAbstract> recordsToSend;
  private ORawBuffer result;

  public OReadRecordResponse() {
  }

  public OReadRecordResponse(
      byte recordType, int version, byte[] record, Set<YTRecordAbstract> recordsToSend) {
    this.recordType = recordType;
    this.version = version;
    this.record = record;
    this.recordsToSend = recordsToSend;
  }

  public void write(YTDatabaseSessionInternal session, OChannelDataOutput network,
      int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    if (record != null) {
      network.writeByte((byte) 1);
      if (protocolVersion <= OChannelBinaryProtocol.PROTOCOL_VERSION_27) {
        network.writeBytes(record);
        network.writeVersion(version);
        network.writeByte(recordType);
      } else {
        network.writeByte(recordType);
        network.writeVersion(version);
        network.writeBytes(record);
      }
      for (YTRecordAbstract d : recordsToSend) {
        if (d.getIdentity().isValid()) {
          network.writeByte((byte) 2); // CLIENT CACHE
          // RECORD. IT ISN'T PART OF THE RESULT SET
          OMessageHelper.writeRecord(session, network, d, serializer);
        }
      }
    }
    // End of the response
    network.writeByte((byte) 0);
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput network,
      OStorageRemoteSession session) throws IOException {
    ORecordSerializerNetworkV37Client serializer = ORecordSerializerNetworkV37Client.INSTANCE;
    if (network.readByte() == 0) {
      return;
    }

    final ORawBuffer buffer;
    final byte type = network.readByte();
    final int recVersion = network.readVersion();
    final byte[] bytes = network.readBytes();
    buffer = new ORawBuffer(bytes, recVersion, type);

    // TODO: This should not be here, move it in a callback or similar
    YTRecordAbstract record;
    while (network.readByte() == 2) {
      record = (YTRecordAbstract) OMessageHelper.readIdentifiable(db, network, serializer);

      if (db != null && record != null) {
        var cacheRecord = db.getLocalCache().findRecord(record.getIdentity());

        if (cacheRecord != record) {
          if (cacheRecord != null) {
            record.copyTo(cacheRecord);
          } else {
            db.getLocalCache().updateRecord(record);
          }
        }
      }
    }
    result = buffer;
  }

  public byte[] getRecord() {
    return record;
  }

  public ORawBuffer getResult() {
    return result;
  }
}
