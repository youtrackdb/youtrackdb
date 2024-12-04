package com.orientechnologies.orient.client.remote.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.client.remote.message.push.OStorageConfigurationPayload;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.Test;

/**
 *
 */
public class ORemotePushMessagesTest extends DBTestBase {

  @Test
  public void testDistributedConfig() throws IOException {
    MockChannel channel = new MockChannel();
    List<String> hosts = new ArrayList<>();
    hosts.add("one");
    hosts.add("two");
    OPushDistributedConfigurationRequest request = new OPushDistributedConfigurationRequest(hosts);
    request.write(null, channel);
    channel.close();

    OPushDistributedConfigurationRequest readRequest = new OPushDistributedConfigurationRequest();
    readRequest.read(db, channel);
    assertEquals(2, readRequest.getHosts().size());
    assertEquals("one", readRequest.getHosts().get(0));
    assertEquals("two", readRequest.getHosts().get(1));
  }

  @Test
  public void testSchema() throws IOException {
    OxygenDB oxygenDB = new OxygenDB(DBTestBase.embeddedDBUrl(getClass()),
        OxygenDBConfig.defaultConfig());
    oxygenDB.execute("create database test memory users (admin identified by 'admin' role admin)");
    var session = (ODatabaseSessionInternal) oxygenDB.open("test", "admin", "admin");

    session.begin();
    ODocument schema =
        session.getSharedContext().getSchema().toStream(session).copy();
    session.commit();

    MockChannel channel = new MockChannel();
    OPushSchemaRequest request = new OPushSchemaRequest(schema);
    request.write(session, channel);
    channel.close();

    OPushSchemaRequest readRequest = new OPushSchemaRequest();
    readRequest.read(session, channel);
    assertNotNull(readRequest.getSchema());
  }

  @Test
  public void testIndexManager() throws IOException {
    try (OxygenDB oxygenDB = new OxygenDB(DBTestBase.embeddedDBUrl(getClass()),
        OxygenDBConfig.defaultConfig())) {
      oxygenDB.execute(
          "create database test memory users (admin identified by 'admin' role admin)");
      try (ODatabaseSession session = oxygenDB.open("test", "admin", "admin")) {
        session.begin();
        ODocument schema =
            ((ODatabaseSessionInternal) session).getSharedContext().getIndexManager()
                .toStream((ODatabaseSessionInternal) session);

        MockChannel channel = new MockChannel();

        OPushIndexManagerRequest request = new OPushIndexManagerRequest(schema);
        request.write(null, channel);
        channel.close();
        session.commit();

        OPushIndexManagerRequest readRequest = new OPushIndexManagerRequest();
        readRequest.read(db, channel);
        assertNotNull(readRequest.getIndexManager());
      }
    }
  }

  @Test
  public void testStorageConfiguration() throws IOException {
    OxygenDB oxygenDB = new OxygenDB(DBTestBase.embeddedDBUrl(getClass()),
        OxygenDBConfig.defaultConfig());
    oxygenDB.execute("create database test memory users (admin identified by 'admin' role admin)");
    ODatabaseSession session = oxygenDB.open("test", "admin", "admin");
    OStorageConfiguration configuration =
        ((ODatabaseSessionInternal) session).getStorage().getConfiguration();
    session.close();
    oxygenDB.close();
    MockChannel channel = new MockChannel();

    OPushStorageConfigurationRequest request = new OPushStorageConfigurationRequest(configuration);
    request.write(null, channel);
    channel.close();

    OPushStorageConfigurationRequest readRequest = new OPushStorageConfigurationRequest();
    readRequest.read(db, channel);
    OStorageConfigurationPayload readPayload = readRequest.getPayload();
    OStorageConfigurationPayload payload = request.getPayload();
    assertEquals(readPayload.getName(), payload.getName());
    assertEquals(readPayload.getDateFormat(), payload.getDateFormat());
    assertEquals(readPayload.getDateTimeFormat(), payload.getDateTimeFormat());
    assertEquals(readPayload.getVersion(), payload.getVersion());
    assertEquals(readPayload.getDirectory(), payload.getDirectory());
    for (OStorageEntryConfiguration readProperty : readPayload.getProperties()) {
      boolean found = false;
      for (OStorageEntryConfiguration property : payload.getProperties()) {
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
    for (OStorageClusterConfiguration readCluster : readPayload.getClusters()) {
      boolean found = false;
      for (OStorageClusterConfiguration cluster : payload.getClusters()) {
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

    OSubscribeRequest request =
        new OSubscribeRequest(new OSubscribeLiveQueryRequest("10", new HashMap<>()));
    request.write(null, channel, null);
    channel.close();

    OSubscribeRequest requestRead = new OSubscribeRequest();
    requestRead.read(db, channel, 1, ORecordSerializerNetworkV37.INSTANCE);

    assertEquals(request.getPushMessage(), requestRead.getPushMessage());
    assertTrue(requestRead.getPushRequest() instanceof OSubscribeLiveQueryRequest);
  }

  @Test
  public void testSubscribeResponse() throws IOException {
    MockChannel channel = new MockChannel();

    OSubscribeResponse response = new OSubscribeResponse(new OSubscribeLiveQueryResponse(10));
    response.write(null, channel, 1, ORecordSerializerNetworkV37.INSTANCE);
    channel.close();

    OSubscribeResponse responseRead = new OSubscribeResponse(new OSubscribeLiveQueryResponse());
    responseRead.read(db, channel, null);

    assertTrue(responseRead.getResponse() instanceof OSubscribeLiveQueryResponse);
    assertEquals(10, ((OSubscribeLiveQueryResponse) responseRead.getResponse()).getMonitorId());
  }

  @Test
  public void testUnsubscribeRequest() throws IOException {
    MockChannel channel = new MockChannel();
    OUnsubscribeRequest request = new OUnsubscribeRequest(new OUnsubscribeLiveQueryRequest(10));
    request.write(null, channel, null);
    channel.close();
    OUnsubscribeRequest readRequest = new OUnsubscribeRequest();
    readRequest.read(db, channel, 0, null);
    assertEquals(
        10, ((OUnsubscribeLiveQueryRequest) readRequest.getUnsubscribeRequest()).getMonitorId());
  }
}
