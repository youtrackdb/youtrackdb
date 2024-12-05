package com.orientechnologies.orient.server.token;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.security.binary.OBinaryToken;
import com.jetbrains.youtrack.db.internal.core.metadata.security.binary.OBinaryTokenPayloadImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.security.binary.OBinaryTokenSerializer;
import com.jetbrains.youtrack.db.internal.core.metadata.security.jwt.OrientJwtHeader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Test;

public class OBinaryTokenSerializerTest {

  private final OBinaryTokenSerializer ser =
      new OBinaryTokenSerializer(
          new String[]{"plocal", "memory"},
          new String[]{"key"},
          new String[]{"HmacSHA256"},
          new String[]{"YouTrackDB"});

  @Test
  public void testSerializerDeserializeToken() throws IOException {
    OBinaryToken token = new OBinaryToken();
    OBinaryTokenPayloadImpl payload = new OBinaryTokenPayloadImpl();
    payload.setDatabase("test");
    payload.setDatabaseType("plocal");
    payload.setUserRid(new YTRecordId(43, 234));
    OrientJwtHeader header = new OrientJwtHeader();
    header.setKeyId("key");
    header.setAlgorithm("HmacSHA256");
    header.setType("YouTrackDB");
    token.setHeader(header);
    payload.setExpiry(20L);
    payload.setProtocolVersion((short) 2);
    payload.setSerializer("ser");
    payload.setDriverName("aa");
    payload.setDriverVersion("aa");
    token.setPayload(payload);
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    ser.serialize(token, bas);
    ByteArrayInputStream input = new ByteArrayInputStream(bas.toByteArray());
    OBinaryToken tok = ser.deserialize(input);

    assertEquals("test", token.getDatabase());
    assertEquals("plocal", token.getDatabaseType());
    YTRID id = token.getUserId();
    assertEquals(43, id.getClusterId());
    assertEquals(20L, tok.getExpiry());

    assertEquals("YouTrackDB", tok.getHeader().getType());
    assertEquals("HmacSHA256", tok.getHeader().getAlgorithm());
    assertEquals("key", tok.getHeader().getKeyId());

    assertEquals((short) 2, tok.getProtocolVersion());
    assertEquals("ser", tok.getSerializer());
    assertEquals("aa", tok.getDriverName());
    assertEquals("aa", tok.getDriverVersion());
  }

  @Test
  public void testSerializerDeserializeServerUserToken() throws IOException {
    OBinaryToken token = new OBinaryToken();
    OBinaryTokenPayloadImpl payload = new OBinaryTokenPayloadImpl();
    payload.setDatabase("test");
    payload.setDatabaseType("plocal");
    payload.setUserRid(new YTRecordId(43, 234));
    OrientJwtHeader header = new OrientJwtHeader();
    header.setKeyId("key");
    header.setAlgorithm("HmacSHA256");
    header.setType("YouTrackDB");
    token.setHeader(header);
    payload.setExpiry(20L);
    payload.setServerUser(true);
    payload.setUserName("aaa");
    payload.setProtocolVersion((short) 2);
    payload.setSerializer("ser");
    payload.setDriverName("aa");
    payload.setDriverVersion("aa");
    token.setPayload(payload);
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    ser.serialize(token, bas);
    ByteArrayInputStream input = new ByteArrayInputStream(bas.toByteArray());
    OBinaryToken tok = ser.deserialize(input);

    assertEquals("test", token.getDatabase());
    assertEquals("plocal", token.getDatabaseType());
    YTRID id = token.getUserId();
    assertEquals(43, id.getClusterId());
    assertEquals(20L, tok.getExpiry());
    assertTrue(token.isServerUser());
    assertEquals("aaa", tok.getUserName());

    assertEquals("YouTrackDB", tok.getHeader().getType());
    assertEquals("HmacSHA256", tok.getHeader().getAlgorithm());
    assertEquals("key", tok.getHeader().getKeyId());

    assertEquals((short) 2, tok.getProtocolVersion());
    assertEquals("ser", tok.getSerializer());
    assertEquals("aa", tok.getDriverName());
    assertEquals("aa", tok.getDriverVersion());
  }

  @Test
  public void testSerializerDeserializeNullInfoUserToken() throws IOException {
    OBinaryToken token = new OBinaryToken();
    OBinaryTokenPayloadImpl payload = new OBinaryTokenPayloadImpl();
    payload.setDatabase(null);
    payload.setDatabaseType(null);
    payload.setUserRid(null);
    OrientJwtHeader header = new OrientJwtHeader();
    header.setKeyId("key");
    header.setAlgorithm("HmacSHA256");
    header.setType("YouTrackDB");
    token.setHeader(header);
    payload.setExpiry(20L);
    payload.setServerUser(true);
    payload.setUserName("aaa");
    payload.setProtocolVersion((short) 2);
    payload.setSerializer("ser");
    payload.setDriverName("aa");
    payload.setDriverVersion("aa");
    token.setPayload(payload);
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    ser.serialize(token, bas);
    ByteArrayInputStream input = new ByteArrayInputStream(bas.toByteArray());
    OBinaryToken tok = ser.deserialize(input);

    assertNull(token.getDatabase());
    assertNull(token.getDatabaseType());
    YTRID id = token.getUserId();
    assertNull(id);
    assertEquals(20L, tok.getExpiry());
    assertTrue(token.isServerUser());
    assertEquals("aaa", tok.getUserName());

    assertEquals("YouTrackDB", tok.getHeader().getType());
    assertEquals("HmacSHA256", tok.getHeader().getAlgorithm());
    assertEquals("key", tok.getHeader().getKeyId());

    assertEquals((short) 2, tok.getProtocolVersion());
    assertEquals("ser", tok.getSerializer());
    assertEquals("aa", tok.getDriverName());
    assertEquals("aa", tok.getDriverVersion());
  }
}
