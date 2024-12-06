package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class OPushDistributedConfigurationRequest
    implements OBinaryPushRequest<OBinaryPushResponse> {

  public EntityImpl configuration;
  private List<String> hosts;

  public OPushDistributedConfigurationRequest(List<String> hosts) {
    this.hosts = hosts;
  }

  public OPushDistributedConfigurationRequest() {
  }

  @Override
  public byte getPushCommand() {
    return ChannelBinaryProtocol.REQUEST_PUSH_DISTRIB_CONFIG;
  }

  @Override
  public void write(DatabaseSessionInternal session, ChannelDataOutput channel)
      throws IOException {
    channel.writeInt(hosts.size());
    for (String host : hosts) {
      channel.writeString(host);
    }
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network) throws IOException {
    int size = network.readInt();
    hosts = new ArrayList<>(size);
    while (size-- > 0) {
      hosts.add(network.readString());
    }
  }

  public OBinaryPushResponse execute(DatabaseSessionInternal session, ORemotePushHandler remote) {
    return remote.executeUpdateDistributedConfig(this);
  }

  @Override
  public OBinaryPushResponse createResponse() {
    return null;
  }

  public List<String> getHosts() {
    return hosts;
  }
}
