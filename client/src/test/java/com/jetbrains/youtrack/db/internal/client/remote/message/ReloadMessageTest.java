package com.jetbrains.youtrack.db.internal.client.remote.message;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.client.remote.message.push.StorageConfigurationPayload;
import com.jetbrains.youtrack.db.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import java.io.IOException;
import org.junit.Test;

public class ReloadMessageTest extends DbTestBase {

  @Test
  public void testWriteReadResponse() throws IOException {
    StorageConfiguration configuration =
        db.getStorage().getConfiguration();
    ReloadResponse37 responseWrite = new ReloadResponse37(configuration);
    MockChannel channel = new MockChannel();
    responseWrite.write(db, channel,
        ChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION, null);
    channel.close();
    ReloadResponse37 responseRead = new ReloadResponse37();
    responseRead.read(db, channel, null);
    StorageConfigurationPayload payload = responseRead.getPayload();
    assertEquals(configuration.getProperties().size(), payload.getProperties().size());
    for (int i = 0; i < configuration.getProperties().size(); i++) {
      assertEquals(configuration.getProperties().get(i).name, payload.getProperties().get(i).name);
      assertEquals(
          configuration.getProperties().get(i).value, payload.getProperties().get(i).value);
    }
    assertEquals(configuration.getDateFormat(), payload.getDateFormat());
    assertEquals(configuration.getDateTimeFormat(), payload.getDateTimeFormat());
    assertEquals(configuration.getName(), payload.getName());
    assertEquals(configuration.getVersion(), payload.getVersion());
    assertEquals(configuration.getDirectory(), payload.getDirectory());
    assertEquals(configuration.getSchemaRecordId(), payload.getSchemaRecordId().toString());
    assertEquals(configuration.getIndexMgrRecordId(), payload.getIndexMgrRecordId().toString());
    assertEquals(configuration.getClusterSelection(), payload.getClusterSelection());
    assertEquals(configuration.getConflictStrategy(), payload.getConflictStrategy());
    assertEquals(configuration.isValidationEnabled(), payload.isValidationEnabled());
    assertEquals(configuration.getLocaleLanguage(), payload.getLocaleLanguage());
    assertEquals(configuration.getMinimumClusters(), payload.getMinimumClusters());
    assertEquals(configuration.isStrictSql(), payload.isStrictSql());
    assertEquals(configuration.getCharset(), payload.getCharset());
    assertEquals(configuration.getLocaleCountry(), payload.getLocaleCountry());
    assertEquals(configuration.getTimeZone(), payload.getTimeZone());
    assertEquals(configuration.getRecordSerializer(), payload.getRecordSerializer());
    assertEquals(configuration.getRecordSerializerVersion(), payload.getRecordSerializerVersion());
    assertEquals(configuration.getBinaryFormatVersion(), payload.getBinaryFormatVersion());

    assertEquals(configuration.getClusters().size(), payload.getClusters().size());
    for (int i = 0; i < configuration.getClusters().size(); i++) {
      assertEquals(
          configuration.getClusters().get(i).getId(), payload.getClusters().get(i).getId());
      assertEquals(
          configuration.getClusters().get(i).getName(), payload.getClusters().get(i).getName());
    }
  }
}
