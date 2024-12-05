package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.common.exception.OErrorCode;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class OError37Response implements OBinaryResponse {

  private OErrorCode code;
  private int errorIdentifier;
  private Map<String, String> messages;
  private byte[] verbose;

  public OError37Response(
      OErrorCode code, int errorIdentifier, Map<String, String> messages, byte[] verbose) {
    this.code = code;
    this.errorIdentifier = errorIdentifier;
    this.messages = messages;
    this.verbose = verbose;
  }

  public OError37Response() {
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput network,
      OStorageRemoteSession session) throws IOException {
    int code = network.readInt();
    this.errorIdentifier = network.readInt();
    this.code = OErrorCode.getErrorCode(code);
    messages = new HashMap<>();
    while (network.readByte() == 1) {
      String key = network.readString();
      String value = network.readString();
      messages.put(key, value);
    }
    verbose = network.readBytes();
  }

  @Override
  public void write(YTDatabaseSessionInternal session, OChannelDataOutput channel,
      int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeInt(code.getCode());
    channel.writeInt(errorIdentifier);
    for (Map.Entry<String, String> entry : messages.entrySet()) {
      // MORE DETAILS ARE COMING AS EXCEPTION
      channel.writeByte((byte) 1);

      channel.writeString(entry.getKey());
      channel.writeString(entry.getValue());
    }
    channel.writeByte((byte) 0);

    channel.writeBytes(verbose);
  }

  public int getErrorIdentifier() {
    return errorIdentifier;
  }

  public OErrorCode getCode() {
    return code;
  }

  public Map<String, String> getMessages() {
    return messages;
  }

  public byte[] getVerbose() {
    return verbose;
  }
}
