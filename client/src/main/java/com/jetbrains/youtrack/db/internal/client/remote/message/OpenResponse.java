package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemoteSession;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

public class OpenResponse implements BinaryResponse {

  private int sessionId;
  private byte[] sessionToken;
  private int[] clusterIds;
  private String[] clusterNames;

  private byte[] distributedConfiguration;
  private String serverVersion;

  public OpenResponse() {
  }

  public OpenResponse(
      int sessionId,
      byte[] sessionToken,
      int[] clusterIds,
      String[] clusterNames,
      byte[] distriConf,
      String version) {
    this.sessionId = sessionId;
    this.sessionToken = sessionToken;
    this.clusterIds = clusterIds;
    this.clusterNames = clusterNames;
    this.distributedConfiguration = distriConf;
    this.serverVersion = version;
  }

  @Override
  public void write(DatabaseSessionInternal session, ChannelDataOutput channel,
      int protocolVersion, RecordSerializer serializer)
      throws IOException {
    channel.writeInt(sessionId);
    if (protocolVersion > ChannelBinaryProtocol.PROTOCOL_VERSION_26) {
      channel.writeBytes(sessionToken);
    }

    MessageHelper.writeClustersArray(
        channel, new RawPair<>(clusterNames, clusterIds), protocolVersion);
    channel.writeBytes(distributedConfiguration);
    channel.writeString(serverVersion);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network,
      StorageRemoteSession session) throws IOException {
    sessionId = network.readInt();
    sessionToken = network.readBytes();
    final RawPair<String[], int[]> clusters = MessageHelper.readClustersArray(network);
    distributedConfiguration = network.readBytes();
    serverVersion = network.readString();
  }

  public int getSessionId() {
    return sessionId;
  }

  public byte[] getSessionToken() {
    return sessionToken;
  }

  public int[] getClusterIds() {
    return clusterIds;
  }

  public String[] getClusterNames() {
    return clusterNames;
  }

  public byte[] getDistributedConfiguration() {
    return distributedConfiguration;
  }
}
