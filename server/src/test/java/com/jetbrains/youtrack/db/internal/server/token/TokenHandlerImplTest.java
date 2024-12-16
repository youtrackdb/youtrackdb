package com.jetbrains.youtrack.db.internal.server.token;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.security.SecurityUser;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityUserIml;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Token;
import com.jetbrains.youtrack.db.internal.core.metadata.security.jwt.JwtPayload;
import com.jetbrains.youtrack.db.internal.core.metadata.security.jwt.TokenHeader;
import com.jetbrains.youtrack.db.internal.core.metadata.security.jwt.YouTrackDBJwtHeader;
import com.jetbrains.youtrack.db.internal.server.BaseMemoryInternalDatabase;
import com.jetbrains.youtrack.db.internal.server.network.protocol.NetworkProtocolData;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import org.junit.Test;

public class TokenHandlerImplTest extends BaseMemoryInternalDatabase {

  @Test
  public void testWebTokenCreationValidation()
      throws InvalidKeyException, NoSuchAlgorithmException, IOException {
    SecurityUser original = db.geCurrentUser();
    TokenHandlerImpl handler = new TokenHandlerImpl("any key".getBytes(), 60, "HmacSHA256");
    byte[] token = handler.getSignedWebToken(db, original);

    try {
      // Make this thread wait at least 10 milliseconds before check the validity
      Thread.sleep(10);
    } catch (InterruptedException e) {
    }

    Token tok = handler.parseWebToken(token);

    assertNotNull(tok);

    assertTrue(tok.getIsVerified());

    SecurityUserIml user = tok.getUser(db);
    assertEquals(user.getName(db), original.getName(db));
    boolean boole = handler.validateToken(tok, "open", db.getName());
    assertTrue(boole);
    assertTrue(tok.getIsValid());
  }

  @Test(expected = Exception.class)
  public void testInvalidToken() throws InvalidKeyException, NoSuchAlgorithmException, IOException {
    TokenHandlerImpl handler = new TokenHandlerImpl("any key".getBytes(), 60, "HmacSHA256");
    handler.parseWebToken("random".getBytes());
  }

  @Test
  public void testSerializeDeserializeWebHeader() throws Exception {
    TokenHeader header = new YouTrackDBJwtHeader();
    header.setType("YouTrackDB");
    header.setAlgorithm("some");
    header.setKeyId("the_key");
    TokenHandlerImpl handler = new TokenHandlerImpl();
    byte[] headerbytes = handler.serializeWebHeader(header);

    TokenHeader des = handler.deserializeWebHeader(headerbytes);
    assertNotNull(des);
    assertEquals(header.getType(), des.getType());
    assertEquals(header.getKeyId(), des.getKeyId());
    assertEquals(header.getAlgorithm(), des.getAlgorithm());
    assertEquals(header.getType(), des.getType());
  }

  @Test
  public void testSerializeDeserializeWebPayload() throws Exception {
    YouTrackDBJwtPayload payload = new YouTrackDBJwtPayload();
    String ptype = "YouTrackDB";
    payload.setAudience("audiance");
    payload.setExpiry(1L);
    payload.setIssuedAt(2L);
    payload.setIssuer("YouTrackDB");
    payload.setNotBefore(3L);
    payload.setUserName("the subject");
    payload.setTokenId("aaa");
    payload.setUserRid(new RecordId(3, 4));

    TokenHandlerImpl handler = new TokenHandlerImpl();
    byte[] payloadbytes = handler.serializeWebPayload(payload);

    JwtPayload des = handler.deserializeWebPayload(ptype, payloadbytes);
    assertNotNull(des);
    assertEquals(payload.getAudience(), des.getAudience());
    assertEquals(payload.getExpiry(), des.getExpiry());
    assertEquals(payload.getIssuedAt(), des.getIssuedAt());
    assertEquals(payload.getIssuer(), des.getIssuer());
    assertEquals(payload.getNotBefore(), des.getNotBefore());
    assertEquals(payload.getTokenId(), des.getTokenId());
    assertEquals(payload.getUserName(), des.getUserName());
  }

  @Test
  public void testTokenForge() throws InvalidKeyException, NoSuchAlgorithmException, IOException {
    SecurityUser original = db.geCurrentUser();
    TokenHandlerImpl handler = new TokenHandlerImpl("any key".getBytes(), 60, "HmacSHA256");

    byte[] token = handler.getSignedWebToken(db, original);
    byte[] token2 = handler.getSignedWebToken(db, original);
    String s = new String(token);
    String s2 = new String(token2);

    String newS = s.substring(0, s.lastIndexOf('.')) + s2.substring(s2.lastIndexOf('.'));

    Token tok = handler.parseWebToken(newS.getBytes());

    assertNotNull(tok);

    assertFalse(tok.getIsVerified());
  }

  @Test
  public void testBinartTokenCreationValidation()
      throws InvalidKeyException, NoSuchAlgorithmException, IOException {
    SecurityUser original = db.geCurrentUser();
    TokenHandlerImpl handler = new TokenHandlerImpl("any key".getBytes(), 60, "HmacSHA256");
    NetworkProtocolData data = new NetworkProtocolData();
    data.driverName = "aa";
    data.driverVersion = "aa";
    data.setSerializationImpl("a");
    data.protocolVersion = 2;

    byte[] token = handler.getSignedBinaryToken(db, original, data);

    Token tok = handler.parseBinaryToken(token);

    assertNotNull(tok);

    assertTrue(tok.getIsVerified());

    SecurityUserIml user = tok.getUser(db);
    assertEquals(user.getName(db), original.getName(db));
    boolean boole = handler.validateBinaryToken(tok);
    assertTrue(boole);
    assertTrue(tok.getIsValid());
  }

  @Test
  public void testTokenNotRenew() {
    SecurityUser original = db.geCurrentUser();
    TokenHandlerImpl handler = new TokenHandlerImpl("any key".getBytes(), 60, "HmacSHA256");
    NetworkProtocolData data = new NetworkProtocolData();
    data.driverName = "aa";
    data.driverVersion = "aa";
    data.setSerializationImpl("a");
    data.protocolVersion = 2;

    byte[] token = handler.getSignedBinaryToken(db, original, data);

    Token tok = handler.parseBinaryToken(token);
    token = handler.renewIfNeeded(tok);

    assertEquals(0, token.length);
  }

  @Test
  public void testTokenRenew() {
    SecurityUser original = db.geCurrentUser();
    TokenHandlerImpl handler = new TokenHandlerImpl("any key".getBytes(), 60, "HmacSHA256");
    NetworkProtocolData data = new NetworkProtocolData();
    data.driverName = "aa";
    data.driverVersion = "aa";
    data.setSerializationImpl("a");
    data.protocolVersion = 2;

    byte[] token = handler.getSignedBinaryToken(db, original, data);

    Token tok = handler.parseBinaryToken(token);
    tok.setExpiry(System.currentTimeMillis() + (handler.getSessionInMills() / 2) - 1);
    token = handler.renewIfNeeded(tok);

    assertTrue(token.length != 0);
  }
}
