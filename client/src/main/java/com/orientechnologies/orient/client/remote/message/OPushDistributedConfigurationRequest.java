package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class OPushDistributedConfigurationRequest
    implements OBinaryPushRequest<OBinaryPushResponse> {

  public YTDocument configuration;
  private List<String> hosts;

  public OPushDistributedConfigurationRequest(List<String> hosts) {
    this.hosts = hosts;
  }

  public OPushDistributedConfigurationRequest() {
  }

  @Override
  public byte getPushCommand() {
    return OChannelBinaryProtocol.REQUEST_PUSH_DISTRIB_CONFIG;
  }

  @Override
  public void write(YTDatabaseSessionInternal session, OChannelDataOutput channel)
      throws IOException {
    channel.writeInt(hosts.size());
    for (String host : hosts) {
      channel.writeString(host);
    }
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput network) throws IOException {
    int size = network.readInt();
    hosts = new ArrayList<>(size);
    while (size-- > 0) {
      hosts.add(network.readString());
    }
  }

  public OBinaryPushResponse execute(YTDatabaseSessionInternal session, ORemotePushHandler remote) {
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
