package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class OErrorResponse implements OBinaryResponse {

  private Map<String, String> messages;
  private byte[] result;

  public OErrorResponse() {
  }

  public OErrorResponse(Map<String, String> messages, byte[] result) {
    this.messages = messages;
    this.result = result;
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network,
      OStorageRemoteSession session) throws IOException {
    messages = new HashMap<>();
    while (network.readByte() == 1) {
      String key = network.readString();
      String value = network.readString();
      messages.put(key, value);
    }
    result = network.readBytes();
  }

  @Override
  public void write(DatabaseSessionInternal session, ChannelDataOutput channel,
      int protocolVersion, RecordSerializer serializer)
      throws IOException {
    for (Entry<String, String> entry : messages.entrySet()) {
      // MORE DETAILS ARE COMING AS EXCEPTION
      channel.writeByte((byte) 1);

      channel.writeString(entry.getKey());
      channel.writeString(entry.getValue());
    }
    channel.writeByte((byte) 0);

    channel.writeBytes(result);
  }

  public Map<String, String> getMessages() {
    return messages;
  }

  public byte[] getResult() {
    return result;
  }
}
