package com.orientechnologies.orient.server.token;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.exception.YTSystemException;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.config.YTContextConfiguration;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.id.YTImmutableRecordId;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.OTokenException;
import com.orientechnologies.orient.core.metadata.security.YTSecurityUser;
import com.orientechnologies.orient.core.metadata.security.binary.OBinaryToken;
import com.orientechnologies.orient.core.metadata.security.binary.OBinaryTokenPayloadImpl;
import com.orientechnologies.orient.core.metadata.security.binary.OBinaryTokenSerializer;
import com.orientechnologies.orient.core.metadata.security.jwt.OJwtPayload;
import com.orientechnologies.orient.core.metadata.security.jwt.OTokenHeader;
import com.orientechnologies.orient.core.metadata.security.jwt.OrientJwtHeader;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.security.OParsedToken;
import com.orientechnologies.orient.core.security.OTokenSign;
import com.orientechnologies.orient.core.security.OTokenSignImpl;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OTokenHandler;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;

/**
 *
 */
public class OTokenHandlerImpl implements OTokenHandler {

  protected static final int JWT_DELIMITER = '.';
  private OBinaryTokenSerializer binarySerializer;
  private long sessionInMills = 1000 * 60 * 60; // 1 HOUR
  private final OTokenSign sign;

  public OTokenHandlerImpl(YTContextConfiguration config) {
    this(
        new OTokenSignImpl(config),
        config.getValueAsLong(YTGlobalConfiguration.NETWORK_TOKEN_EXPIRE_TIMEOUT));
  }

  protected OTokenHandlerImpl(byte[] key, long sessionLength, String algorithm) {
    this(new OTokenSignImpl(key, algorithm), sessionLength);
  }

  public OTokenHandlerImpl(OTokenSign sign, YTContextConfiguration config) {
    this(sign, config.getValueAsLong(YTGlobalConfiguration.NETWORK_TOKEN_EXPIRE_TIMEOUT));
  }

  protected OTokenHandlerImpl(OTokenSign sign, long sessionLength) {
    this.sign = sign;
    sessionInMills = sessionLength * 1000 * 60;
    this.binarySerializer =
        new OBinaryTokenSerializer(
            new String[]{"plocal", "memory"},
            this.sign.getKeys(),
            new String[]{this.sign.getAlgorithm()},
            new String[]{"YouTrackDB", "node"});
  }

  protected OTokenHandlerImpl() {
    this.sign = null;
  }

  @Override
  public OToken parseWebToken(byte[] tokenBytes) {
    OParsedToken parsedToken = parseOnlyWebToken(tokenBytes);
    OToken token = parsedToken.getToken();
    token.setIsVerified(this.sign.verifyTokenSign(parsedToken));
    return token;
  }

  @Override
  public OParsedToken parseOnlyWebToken(byte[] tokenBytes) {
    JsonWebToken token = null;

    // / <header>.<payload>.<signature>
    int firstDot = -1;
    int secondDot = -1;
    for (int x = 0; x < tokenBytes.length; x++) {
      if (tokenBytes[x] == JWT_DELIMITER) {
        if (firstDot == -1) {
          firstDot = x; // stores reference to first '.' character in JWT token
        } else {
          secondDot = x;
          break;
        }
      }
    }

    if (firstDot == -1) {
      throw new RuntimeException("Token data too short: missed header");
    }

    if (secondDot == -1) {
      throw new RuntimeException("Token data too short: missed signature");
    }
    final byte[] decodedHeader =
        Base64.getUrlDecoder().decode(ByteBuffer.wrap(tokenBytes, 0, firstDot)).array();
    final byte[] decodedPayload =
        Base64.getUrlDecoder()
            .decode(ByteBuffer.wrap(tokenBytes, firstDot + 1, secondDot - (firstDot + 1)))
            .array();
    final byte[] decodedSignature =
        Base64.getUrlDecoder()
            .decode(ByteBuffer.wrap(tokenBytes, secondDot + 1, tokenBytes.length - (secondDot + 1)))
            .array();

    final OrientJwtHeader header = deserializeWebHeader(decodedHeader);
    final OJwtPayload deserializeWebPayload =
        deserializeWebPayload(header.getType(), decodedPayload);
    token = new JsonWebToken(header, deserializeWebPayload);
    byte[] onlyTokenBytes = new byte[secondDot];
    System.arraycopy(tokenBytes, 0, onlyTokenBytes, 0, secondDot);
    return new OParsedToken(token, onlyTokenBytes, decodedSignature);
  }

  @Override
  public boolean validateToken(OParsedToken token, String command, String database) {
    if (!token.getToken().getIsVerified()) {
      boolean value = this.sign.verifyTokenSign(token);
      token.getToken().setIsVerified(value);
    }
    return token.getToken().getIsVerified() && validateToken(token.getToken(), command, database);
  }

  @Override
  public boolean validateToken(final OToken token, final String command, final String database) {
    boolean valid = false;
    if (!(token instanceof JsonWebToken)) {
      return false;
    }
    final long curTime = System.currentTimeMillis();
    if (token.getDatabase().equalsIgnoreCase(database)
        && token.getExpiry() > curTime
        && (token.getExpiry() - (sessionInMills + 1)) < curTime) {
      valid = true;
    }
    token.setIsValid(valid);
    return valid;
  }

  @Override
  public boolean validateBinaryToken(OParsedToken token) {
    if (!token.getToken().getIsVerified()) {
      boolean value = this.sign.verifyTokenSign(token);
      token.getToken().setIsVerified(value);
    }
    return token.getToken().getIsVerified() && validateBinaryToken(token.getToken());
  }

  @Override
  public boolean validateBinaryToken(final OToken token) {
    boolean valid = false;
    // The "node" token is for backward compatibility for old ditributed binary, may be removed if
    // we do not support runtime compatiblity with 3.1 or less
    if ("node".equals(token.getHeader().getType())) {
      valid = true;
    } else {
      final long curTime = System.currentTimeMillis();
      if (token.getExpiry() > curTime && (token.getExpiry() - (sessionInMills + 1)) < curTime) {
        valid = true;
      }
    }
    token.setIsValid(valid);

    return valid;
  }

  public byte[] getSignedWebToken(final YTDatabaseSessionInternal db, final YTSecurityUser user) {
    final ByteArrayOutputStream tokenByteOS = new ByteArrayOutputStream(1024);
    final OrientJwtHeader header = new OrientJwtHeader();
    header.setAlgorithm("HS256");
    header.setKeyId("");

    final OJwtPayload payload = createPayload(db, user);
    header.setType(getPayloadType(payload));
    try {
      byte[] bytes = serializeWebHeader(header);
      tokenByteOS.write(
          Base64.getUrlEncoder().encode(ByteBuffer.wrap(bytes, 0, bytes.length)).array());
      tokenByteOS.write(JWT_DELIMITER);
      bytes = serializeWebPayload(payload);
      tokenByteOS.write(
          Base64.getUrlEncoder().encode(ByteBuffer.wrap(bytes, 0, bytes.length)).array());
      byte[] unsignedToken = tokenByteOS.toByteArray();
      tokenByteOS.write(JWT_DELIMITER);

      bytes = this.sign.signToken(header, unsignedToken);
      tokenByteOS.write(
          Base64.getUrlEncoder().encode(ByteBuffer.wrap(bytes, 0, bytes.length)).array());
    } catch (Exception ex) {
      throw YTException.wrapException(new YTSystemException("Error on token parsing"), ex);
    }

    return tokenByteOS.toByteArray();
  }

  public byte[] getSignedWebTokenServerUser(final YTSecurityUser user) {
    final ByteArrayOutputStream tokenByteOS = new ByteArrayOutputStream(1024);
    final OrientJwtHeader header = new OrientJwtHeader();
    header.setAlgorithm("HS256");
    header.setKeyId("");

    final OJwtPayload payload = createPayloadServerUser(user);
    header.setType(getPayloadType(payload));
    try {
      byte[] bytes = serializeWebHeader(header);
      tokenByteOS.write(
          Base64.getUrlEncoder().encode(ByteBuffer.wrap(bytes, 0, bytes.length)).array());
      tokenByteOS.write(JWT_DELIMITER);
      bytes = serializeWebPayload(payload);
      tokenByteOS.write(
          Base64.getUrlEncoder().encode(ByteBuffer.wrap(bytes, 0, bytes.length)).array());
      byte[] unsignedToken = tokenByteOS.toByteArray();
      tokenByteOS.write(JWT_DELIMITER);

      bytes = this.sign.signToken(header, unsignedToken);
      tokenByteOS.write(
          Base64.getUrlEncoder().encode(ByteBuffer.wrap(bytes, 0, bytes.length)).array());
    } catch (Exception ex) {
      throw YTException.wrapException(new YTSystemException("Error on token parsing"), ex);
    }

    return tokenByteOS.toByteArray();
  }

  @Override
  public boolean validateServerUserToken(OToken token, String command, String database) {
    boolean valid = false;
    if (!(token instanceof JsonWebToken)) {
      return false;
    }
    final OrientJwtPayload payload = (OrientJwtPayload) ((JsonWebToken) token).getPayload();
    if (token.isNowValid()) {
      valid = true;
    }
    token.setIsValid(valid);
    return valid;
  }

  public byte[] getSignedBinaryToken(
      final YTDatabaseSessionInternal db,
      final YTSecurityUser user,
      final ONetworkProtocolData data) {
    try {

      final OBinaryToken token = new OBinaryToken();

      long curTime = System.currentTimeMillis();

      final OrientJwtHeader header = new OrientJwtHeader();
      header.setAlgorithm(this.sign.getAlgorithm());
      header.setKeyId(this.sign.getDefaultKey());
      header.setType("YouTrackDB");
      token.setHeader(header);
      OBinaryTokenPayloadImpl payload = new OBinaryTokenPayloadImpl();
      if (db != null) {
        payload.setDatabase(db.getName());
        payload.setDatabaseType(db.getStorage().getType());
      }
      if (data.serverUser) {
        payload.setServerUser(true);
        payload.setUserName(data.serverUsername);
      }
      if (user != null) {
        payload.setUserRid(user.getIdentity(db).getIdentity());
      }
      payload.setExpiry(curTime + sessionInMills);
      payload.setProtocolVersion(data.protocolVersion);
      payload.setSerializer(data.getSerializationImpl());
      payload.setDriverName(data.driverName);
      payload.setDriverVersion(data.driverVersion);
      token.setPayload(payload);

      return serializeSignedToken(token);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw YTException.wrapException(new YTSystemException("Error on token parsing"), e);
    }
  }

  private byte[] serializeSignedToken(OBinaryToken token) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    binarySerializer.serialize(token, baos);

    final byte[] signature = this.sign.signToken(token.getHeader(), baos.toByteArray());
    baos.write(signature);

    return baos.toByteArray();
  }

  public ONetworkProtocolData getProtocolDataFromToken(
      OClientConnection connection, final OToken token) {
    if (token instanceof OBinaryToken binary) {
      final ONetworkProtocolData data = new ONetworkProtocolData();
      // data.clientId = binary.get;
      data.protocolVersion = binary.getProtocolVersion();
      data.setSerializationImpl(binary.getSerializer());
      data.driverName = binary.getDriverName();
      data.driverVersion = binary.getDriverVersion();
      data.serverUser = binary.isServerUser();
      data.serverUsername = binary.getUserName();
      data.serverUsername = binary.getUserName();
      data.supportsLegacyPushMessages = connection.getData().supportsLegacyPushMessages;
      data.collectStats = connection.getData().collectStats;
      return data;
    }
    return null;
  }

  @Override
  public OToken parseNotVerifyBinaryToken(byte[] binaryToken) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(binaryToken);
    return deserializeBinaryToken(bais);
  }

  @Override
  public OParsedToken parseOnlyBinary(byte[] binaryToken) {
    try {
      final ByteArrayInputStream bais = new ByteArrayInputStream(binaryToken);

      final OBinaryToken token = deserializeBinaryToken(bais);
      final int end = binaryToken.length - bais.available();
      final byte[] decodedSignature = new byte[bais.available()];
      bais.read(decodedSignature);
      byte[] onlyTokenBytes = new byte[end];
      System.arraycopy(binaryToken, 0, onlyTokenBytes, 0, end);
      return new OParsedToken(token, onlyTokenBytes, decodedSignature);
    } catch (Exception e) {
      throw YTException.wrapException(new YTSystemException("Error on token parsing"), e);
    }
  }

  public OToken parseBinaryToken(final byte[] binaryToken) {
    OParsedToken parsedToken = parseOnlyBinary(binaryToken);
    OToken token = parsedToken.getToken();
    token.setIsVerified(this.sign.verifyTokenSign(parsedToken));
    return token;
  }

  @Override
  public byte[] renewIfNeeded(final OToken token) {
    if (token == null) {
      throw new IllegalArgumentException("Token is null");
    }

    final long curTime = System.currentTimeMillis();
    if (token.getExpiry() - curTime < (sessionInMills / 2) && token.getExpiry() >= curTime) {
      final long expiryMinutes = sessionInMills;
      final long currTime = System.currentTimeMillis();
      token.setExpiry(currTime + expiryMinutes);
      try {
        if (token instanceof OBinaryToken) {
          return serializeSignedToken((OBinaryToken) token);
        } else {
          throw new OTokenException("renew of web token not supported");
        }
      } catch (IOException e) {
        throw YTException.wrapException(new YTSystemException("Error on token parsing"), e);
      }
    }
    return OCommonConst.EMPTY_BYTE_ARRAY;
  }

  public long getSessionInMills() {
    return sessionInMills;
  }

  public boolean isEnabled() {
    return true;
  }

  protected OrientJwtHeader deserializeWebHeader(final byte[] decodedHeader) {
    final YTEntityImpl doc = new YTEntityImpl();
    doc.fromJSON(new String(decodedHeader, StandardCharsets.UTF_8));
    final OrientJwtHeader header = new OrientJwtHeader();
    header.setType(doc.field("typ"));
    header.setAlgorithm(doc.field("alg"));
    header.setKeyId(doc.field("kid"));
    return header;
  }

  protected OJwtPayload deserializeWebPayload(final String type, final byte[] decodedPayload) {
    if (!"YouTrackDB".equals(type)) {
      throw new YTSystemException("Payload class not registered:" + type);
    }
    final YTEntityImpl doc = new YTEntityImpl();
    doc.fromJSON(new String(decodedPayload, StandardCharsets.UTF_8));
    final OrientJwtPayload payload = new OrientJwtPayload();
    payload.setUserName(doc.field("username"));
    payload.setIssuer(doc.field("iss"));
    payload.setExpiry(doc.field("exp"));
    payload.setIssuedAt(doc.field("iat"));
    payload.setNotBefore(doc.field("nbf"));
    payload.setDatabase(doc.field("sub"));
    payload.setAudience(doc.field("aud"));
    payload.setTokenId(doc.field("jti"));
    final int cluster = doc.field("uidc");
    final long pos = doc.field("uidp");
    payload.setUserRid(new YTRecordId(cluster, pos));
    payload.setDatabaseType(doc.field("bdtyp"));
    return payload;
  }

  protected byte[] serializeWebHeader(final OTokenHeader header) throws Exception {
    if (header == null) {
      throw new IllegalArgumentException("Token header is null");
    }

    YTEntityImpl doc = new YTEntityImpl();
    doc.field("typ", header.getType());
    doc.field("alg", header.getAlgorithm());
    doc.field("kid", header.getKeyId());
    return doc.toJSON().getBytes(StandardCharsets.UTF_8);
  }

  protected byte[] serializeWebPayload(final OJwtPayload payload) throws Exception {
    if (payload == null) {
      throw new IllegalArgumentException("Token payload is null");
    }

    final YTEntityImpl doc = new YTEntityImpl();
    doc.field("username", payload.getUserName());
    doc.field("iss", payload.getIssuer());
    doc.field("exp", payload.getExpiry());
    doc.field("iat", payload.getIssuedAt());
    doc.field("nbf", payload.getNotBefore());
    doc.field("sub", payload.getDatabase());
    doc.field("aud", payload.getAudience());
    doc.field("jti", payload.getTokenId());
    doc.field("uidc", payload.getUserRid().getClusterId());
    doc.field("uidp", payload.getUserRid().getClusterPosition());
    doc.field("bdtyp", payload.getDatabaseType());
    return doc.toJSON().getBytes(StandardCharsets.UTF_8);
  }

  protected OJwtPayload createPayloadServerUser(YTSecurityUser serverUser) {
    if (serverUser == null) {
      throw new IllegalArgumentException("User is null");
    }

    final OrientJwtPayload payload = new OrientJwtPayload();
    payload.setAudience("YouTrackDBServer");
    payload.setDatabase("-");
    payload.setUserRid(YTImmutableRecordId.EMPTY_RECORD_ID);

    final long expiryMinutes = sessionInMills;
    final long currTime = System.currentTimeMillis();
    payload.setIssuedAt(currTime);
    payload.setNotBefore(currTime);
    payload.setUserName(serverUser.getName(null));
    payload.setTokenId(UUID.randomUUID().toString());
    payload.setExpiry(currTime + expiryMinutes);
    return payload;
  }

  protected OJwtPayload createPayload(final YTDatabaseSessionInternal db,
      final YTSecurityUser user) {
    if (user == null) {
      throw new IllegalArgumentException("User is null");
    }

    final OrientJwtPayload payload = new OrientJwtPayload();
    payload.setAudience("YouTrackDB");
    payload.setDatabase(db.getName());
    payload.setUserRid(user.getIdentity(db).getIdentity());

    final long expiryMinutes = sessionInMills;
    final long currTime = System.currentTimeMillis();
    payload.setIssuedAt(currTime);
    payload.setNotBefore(currTime);
    payload.setUserName(user.getName(db));
    payload.setTokenId(UUID.randomUUID().toString());
    payload.setExpiry(currTime + expiryMinutes);
    return payload;
  }

  protected String getPayloadType(final OJwtPayload payload) {
    return "YouTrackDB";
  }

  private OBinaryToken deserializeBinaryToken(final InputStream bais) {
    try {
      return binarySerializer.deserialize(bais);
    } catch (Exception e) {
      throw YTException.wrapException(new YTSystemException("Cannot deserialize binary token"), e);
    }
  }

  public void setSessionInMills(long sessionInMills) {
    this.sessionInMills = sessionInMills;
  }
}
