package com.jetbrains.youtrack.db.internal.client.remote.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.client.remote.message.push.StorageConfigurationPayload;
import com.jetbrains.youtrack.db.internal.core.config.StorageClusterConfiguration;
import com.jetbrains.youtrack.db.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.config.StorageEntryConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.Test;

/**
 *
 */
public class RemotePushMessagesTest extends DbTestBase {

  @Test
  public void testDistributedConfig() throws IOException {
    MockChannel channel = new MockChannel();
    List<String> hosts = new ArrayList<>();
    hosts.add("one");
    hosts.add("two");
    PushDistributedConfigurationRequest request = new PushDistributedConfigurationRequest(hosts);
    request.write(null, channel);
    channel.close();

    PushDistributedConfigurationRequest readRequest = new PushDistributedConfigurationRequest();
    readRequest.read(db, channel);
    assertEquals(2, readRequest.getHosts().size());
    assertEquals("one", readRequest.getHosts().get(0));
    assertEquals("two", readRequest.getHosts().get(1));
  }

  @Test
  public void testSchema() throws IOException {
    YouTrackDB youTrackDB = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database test memory users (admin identified by 'admin' role admin)");
    var session = (DatabaseSessionInternal) youTrackDB.open("test", "admin", "admin");

    session.begin();
    EntityImpl schema =
        session.getSharedContext().getSchema().toStream(session).copy();
    session.commit();

    MockChannel channel = new MockChannel();
    PushSchemaRequest request = new PushSchemaRequest(schema);
    request.write(session, channel);
    channel.close();

    PushSchemaRequest readRequest = new PushSchemaRequest();
    readRequest.read(session, channel);
    assertNotNull(readRequest.getSchema());
  }

  @Test
  public void testStorageConfiguration() throws IOException {
    YouTrackDB youTrackDB = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database test memory users (admin identified by 'admin' role admin)");
    DatabaseSession session = youTrackDB.open("test", "admin", "admin");
    StorageConfiguration configuration =
        ((DatabaseSessionInternal) session).getStorage().getConfiguration();
    session.close();
    youTrackDB.close();
    MockChannel channel = new MockChannel();

    PushStorageConfigurationRequest request = new PushStorageConfigurationRequest(configuration);
    request.write(null, channel);
    channel.close();

    PushStorageConfigurationRequest readRequest = new PushStorageConfigurationRequest();
    readRequest.read(db, channel);
    StorageConfigurationPayload readPayload = readRequest.getPayload();
    StorageConfigurationPayload payload = request.getPayload();
    assertEquals(readPayload.getName(), payload.getName());
    assertEquals(readPayload.getDateFormat(), payload.getDateFormat());
    assertEquals(readPayload.getDateTimeFormat(), payload.getDateTimeFormat());
    assertEquals(readPayload.getVersion(), payload.getVersion());
    assertEquals(readPayload.getDirectory(), payload.getDirectory());
    for (StorageEntryConfiguration readProperty : readPayload.getProperties()) {
      boolean found = false;
      for (StorageEntryConfiguration property : payload.getProperties()) {
        if (readProperty.name.equals(property.name) && readProperty.value.equals(property.value)) {
          found = true;
          break;
        }
      }
      assertTrue(found);
    }
    assertEquals(readPayload.getSchemaRecordId(), payload.getSchemaRecordId());
    assertEquals(readPayload.getIndexMgrRecordId(), payload.getIndexMgrRecordId());
    assertEquals(readPayload.getClusterSelection(), payload.getClusterSelection());
    assertEquals(readPayload.getConflictStrategy(), payload.getConflictStrategy());
    assertEquals(readPayload.isValidationEnabled(), payload.isValidationEnabled());
    assertEquals(readPayload.getLocaleLanguage(), payload.getLocaleLanguage());
    assertEquals(readPayload.getMinimumClusters(), payload.getMinimumClusters());
    assertEquals(readPayload.isStrictSql(), payload.isStrictSql());
    assertEquals(readPayload.getCharset(), payload.getCharset());
    assertEquals(readPayload.getTimeZone(), payload.getTimeZone());
    assertEquals(readPayload.getLocaleCountry(), payload.getLocaleCountry());
    assertEquals(readPayload.getRecordSerializer(), payload.getRecordSerializer());
    assertEquals(readPayload.getRecordSerializerVersion(), payload.getRecordSerializerVersion());
    assertEquals(readPayload.getBinaryFormatVersion(), payload.getBinaryFormatVersion());
    for (StorageClusterConfiguration readCluster : readPayload.getClusters()) {
      boolean found = false;
      for (StorageClusterConfiguration cluster : payload.getClusters()) {
        if (readCluster.getName().equals(cluster.getName())
            && readCluster.getId() == cluster.getId()) {
          found = true;
          break;
        }
      }
      assertTrue(found);
    }
  }

  @Test
  public void testSubscribeRequest() throws IOException {
    MockChannel channel = new MockChannel();

    SubscribeRequest request =
        new SubscribeRequest(new SubscribeLiveQueryRequest("10", new HashMap<>()));
    request.write(null, channel, null);
    channel.close();

    SubscribeRequest requestRead = new SubscribeRequest();
    requestRead.read(db, channel, 1, RecordSerializerNetworkV37.INSTANCE);

    assertEquals(request.getPushMessage(), requestRead.getPushMessage());
    assertTrue(requestRead.getPushRequest() instanceof SubscribeLiveQueryRequest);
  }

  @Test
  public void testSubscribeResponse() throws IOException {
    MockChannel channel = new MockChannel();

    SubscribeResponse response = new SubscribeResponse(new SubscribeLiveQueryResponse(10));
    response.write(null, channel, 1, RecordSerializerNetworkV37.INSTANCE);
    channel.close();

    SubscribeResponse responseRead = new SubscribeResponse(new SubscribeLiveQueryResponse());
    responseRead.read(db, channel, null);

    assertTrue(responseRead.getResponse() instanceof SubscribeLiveQueryResponse);
    assertEquals(10, ((SubscribeLiveQueryResponse) responseRead.getResponse()).getMonitorId());
  }

  @Test
  public void testUnsubscribeRequest() throws IOException {
    MockChannel channel = new MockChannel();
    UnsubscribeRequest request = new UnsubscribeRequest(new UnsubscribeLiveQueryRequest(10));
    request.write(null, channel, null);
    channel.close();
    UnsubscribeRequest readRequest = new UnsubscribeRequest();
    readRequest.read(db, channel, 0, null);
    assertEquals(
        10, ((UnsubscribeLiveQueryRequest) readRequest.getUnsubscribeRequest()).getMonitorId());
  }
}
