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
import com.orientechnologies.orient.client.remote.message.push.OStorageConfigurationPayload;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OReloadResponse37 implements OBinaryResponse {

  private final OStorageConfigurationPayload payload;

  public OReloadResponse37() {
    payload = new OStorageConfigurationPayload();
  }

  public OReloadResponse37(OStorageConfiguration configuration) {
    payload = new OStorageConfigurationPayload(configuration);
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    payload.read(network);
  }

  public void write(ODatabaseSessionInternal session, OChannelDataOutput channel,
      int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    payload.write(channel);
  }

  public OStorageConfigurationPayload getPayload() {
    return payload;
  }
}
