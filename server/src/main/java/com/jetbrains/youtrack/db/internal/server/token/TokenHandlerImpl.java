package com.jetbrains.youtrack.db.internal.server.token;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.exception.SystemException;
import com.jetbrains.youtrack.db.internal.common.util.CommonConst;
import com.jetbrains.youtrack.db.api.config.ContextConfiguration;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.ImmutableRecordId;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.api.security.SecurityUser;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Token;
import com.jetbrains.youtrack.db.internal.core.metadata.security.TokenException;
import com.jetbrains.youtrack.db.internal.core.metadata.security.binary.BinaryToken;
import com.jetbrains.youtrack.db.internal.core.metadata.security.binary.BinaryTokenPayloadImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.security.binary.BinaryTokenSerializer;
import com.jetbrains.youtrack.db.internal.core.metadata.security.jwt.JwtPayload;
import com.jetbrains.youtrack.db.internal.core.metadata.security.jwt.TokenHeader;
import com.jetbrains.youtrack.db.internal.core.metadata.security.jwt.YouTrackDBJwtHeader;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.security.ParsedToken;
import com.jetbrains.youtrack.db.internal.core.security.TokenSign;
import com.jetbrains.youtrack.db.internal.core.security.TokenSignImpl;
import com.jetbrains.youtrack.db.internal.server.ClientConnection;
import com.jetbrains.youtrack.db.internal.server.OTokenHandler;
import com.jetbrains.youtrack.db.internal.server.network.protocol.NetworkProtocolData;
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
public class TokenHandlerImpl implements OTokenHandler {

  protected static final int JWT_DELIMITER = '.';
  private BinaryTokenSerializer binarySerializer;
  private long sessionInMills = 1000 * 60 * 60; // 1 HOUR
  private final TokenSign sign;

  public TokenHandlerImpl(ContextConfiguration config) {
    this(
        new TokenSignImpl(config),
        config.getValueAsLong(GlobalConfiguration.NETWORK_TOKEN_EXPIRE_TIMEOUT));
  }

  protected TokenHandlerImpl(byte[] key, long sessionLength, String algorithm) {
    this(new TokenSignImpl(key, algorithm), sessionLength);
  }

  public TokenHandlerImpl(TokenSign sign, ContextConfiguration config) {
    this(sign, config.getValueAsLong(GlobalConfiguration.NETWORK_TOKEN_EXPIRE_TIMEOUT));
  }

  protected TokenHandlerImpl(TokenSign sign, long sessionLength) {
    this.sign = sign;
    sessionInMills = sessionLength * 1000 * 60;
    this.binarySerializer =
        new BinaryTokenSerializer(
            new String[]{"plocal", "memory"},
            this.sign.getKeys(),
            new String[]{this.sign.getAlgorithm()},
            new String[]{"YouTrackDB", "node"});
  }

  protected TokenHandlerImpl() {
    this.sign = null;
  }

  @Override
  public Token parseWebToken(byte[] tokenBytes) {
    ParsedToken parsedToken = parseOnlyWebToken(tokenBytes);
    Token token = parsedToken.getToken();
    token.setIsVerified(this.sign.verifyTokenSign(parsedToken));
    return token;
  }

  @Override
  public ParsedToken parseOnlyWebToken(byte[] tokenBytes) {
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

    final YouTrackDBJwtHeader header = deserializeWebHeader(decodedHeader);
    final JwtPayload deserializeWebPayload =
        deserializeWebPayload(header.getType(), decodedPayload);
    token = new JsonWebToken(header, deserializeWebPayload);
    byte[] onlyTokenBytes = new byte[secondDot];
    System.arraycopy(tokenBytes, 0, onlyTokenBytes, 0, secondDot);
    return new ParsedToken(token, onlyTokenBytes, decodedSignature);
  }

  @Override
  public boolean validateToken(ParsedToken token, String command, String database) {
    if (!token.getToken().getIsVerified()) {
      boolean value = this.sign.verifyTokenSign(token);
      token.getToken().setIsVerified(value);
    }
    return token.getToken().getIsVerified() && validateToken(token.getToken(), command, database);
  }

  @Override
  public boolean validateToken(final Token token, final String command, final String database) {
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
  public boolean validateBinaryToken(ParsedToken token) {
    if (!token.getToken().getIsVerified()) {
      boolean value = this.sign.verifyTokenSign(token);
      token.getToken().setIsVerified(value);
    }
    return token.getToken().getIsVerified() && validateBinaryToken(token.getToken());
  }

  @Override
  public boolean validateBinaryToken(final Token token) {
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

  public byte[] getSignedWebToken(final DatabaseSessionInternal db, final SecurityUser user) {
    final ByteArrayOutputStream tokenByteOS = new ByteArrayOutputStream(1024);
    final YouTrackDBJwtHeader header = new YouTrackDBJwtHeader();
    header.setAlgorithm("HS256");
    header.setKeyId("");

    final JwtPayload payload = createPayload(db, user);
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
      throw BaseException.wrapException(new SystemException("Error on token parsing"), ex);
    }

    return tokenByteOS.toByteArray();
  }

  public byte[] getSignedWebTokenServerUser(final SecurityUser user) {
    final ByteArrayOutputStream tokenByteOS = new ByteArrayOutputStream(1024);
    final YouTrackDBJwtHeader header = new YouTrackDBJwtHeader();
    header.setAlgorithm("HS256");
    header.setKeyId("");

    final JwtPayload payload = createPayloadServerUser(user);
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
      throw BaseException.wrapException(new SystemException("Error on token parsing"), ex);
    }

    return tokenByteOS.toByteArray();
  }

  @Override
  public boolean validateServerUserToken(Token token, String command, String database) {
    boolean valid = false;
    if (!(token instanceof JsonWebToken)) {
      return false;
    }
    final YouTrackDBJwtPayload payload = (YouTrackDBJwtPayload) ((JsonWebToken) token).getPayload();
    if (token.isNowValid()) {
      valid = true;
    }
    token.setIsValid(valid);
    return valid;
  }

  public byte[] getSignedBinaryToken(
      final DatabaseSessionInternal db,
      final SecurityUser user,
      final NetworkProtocolData data) {
    try {

      final BinaryToken token = new BinaryToken();

      long curTime = System.currentTimeMillis();

      final YouTrackDBJwtHeader header = new YouTrackDBJwtHeader();
      header.setAlgorithm(this.sign.getAlgorithm());
      header.setKeyId(this.sign.getDefaultKey());
      header.setType("YouTrackDB");
      token.setHeader(header);
      BinaryTokenPayloadImpl payload = new BinaryTokenPayloadImpl();
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
      throw BaseException.wrapException(new SystemException("Error on token parsing"), e);
    }
  }

  private byte[] serializeSignedToken(BinaryToken token) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    binarySerializer.serialize(token, baos);

    final byte[] signature = this.sign.signToken(token.getHeader(), baos.toByteArray());
    baos.write(signature);

    return baos.toByteArray();
  }

  public NetworkProtocolData getProtocolDataFromToken(
      ClientConnection connection, final Token token) {
    if (token instanceof BinaryToken binary) {
      final NetworkProtocolData data = new NetworkProtocolData();
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
  public Token parseNotVerifyBinaryToken(byte[] binaryToken) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(binaryToken);
    return deserializeBinaryToken(bais);
  }

  @Override
  public ParsedToken parseOnlyBinary(byte[] binaryToken) {
    try {
      final ByteArrayInputStream bais = new ByteArrayInputStream(binaryToken);

      final BinaryToken token = deserializeBinaryToken(bais);
      final int end = binaryToken.length - bais.available();
      final byte[] decodedSignature = new byte[bais.available()];
      bais.read(decodedSignature);
      byte[] onlyTokenBytes = new byte[end];
      System.arraycopy(binaryToken, 0, onlyTokenBytes, 0, end);
      return new ParsedToken(token, onlyTokenBytes, decodedSignature);
    } catch (Exception e) {
      throw BaseException.wrapException(new SystemException("Error on token parsing"), e);
    }
  }

  public Token parseBinaryToken(final byte[] binaryToken) {
    ParsedToken parsedToken = parseOnlyBinary(binaryToken);
    Token token = parsedToken.getToken();
    token.setIsVerified(this.sign.verifyTokenSign(parsedToken));
    return token;
  }

  @Override
  public byte[] renewIfNeeded(final Token token) {
    if (token == null) {
      throw new IllegalArgumentException("Token is null");
    }

    final long curTime = System.currentTimeMillis();
    if (token.getExpiry() - curTime < (sessionInMills / 2) && token.getExpiry() >= curTime) {
      final long expiryMinutes = sessionInMills;
      final long currTime = System.currentTimeMillis();
      token.setExpiry(currTime + expiryMinutes);
      try {
        if (token instanceof BinaryToken) {
          return serializeSignedToken((BinaryToken) token);
        } else {
          throw new TokenException("renew of web token not supported");
        }
      } catch (IOException e) {
        throw BaseException.wrapException(new SystemException("Error on token parsing"), e);
      }
    }
    return CommonConst.EMPTY_BYTE_ARRAY;
  }

  public long getSessionInMills() {
    return sessionInMills;
  }

  public boolean isEnabled() {
    return true;
  }

  protected YouTrackDBJwtHeader deserializeWebHeader(final byte[] decodedHeader) {
    final EntityImpl entity = new EntityImpl();
    entity.fromJSON(new String(decodedHeader, StandardCharsets.UTF_8));
    final YouTrackDBJwtHeader header = new YouTrackDBJwtHeader();
    header.setType(entity.field("typ"));
    header.setAlgorithm(entity.field("alg"));
    header.setKeyId(entity.field("kid"));
    return header;
  }

  protected JwtPayload deserializeWebPayload(final String type, final byte[] decodedPayload) {
    if (!"YouTrackDB".equals(type)) {
      throw new SystemException("Payload class not registered:" + type);
    }
    final EntityImpl entity = new EntityImpl();
    entity.fromJSON(new String(decodedPayload, StandardCharsets.UTF_8));
    final YouTrackDBJwtPayload payload = new YouTrackDBJwtPayload();
    payload.setUserName(entity.field("username"));
    payload.setIssuer(entity.field("iss"));
    payload.setExpiry(entity.field("exp"));
    payload.setIssuedAt(entity.field("iat"));
    payload.setNotBefore(entity.field("nbf"));
    payload.setDatabase(entity.field("sub"));
    payload.setAudience(entity.field("aud"));
    payload.setTokenId(entity.field("jti"));
    final int cluster = entity.field("uidc");
    final long pos = entity.field("uidp");
    payload.setUserRid(new RecordId(cluster, pos));
    payload.setDatabaseType(entity.field("bdtyp"));
    return payload;
  }

  protected byte[] serializeWebHeader(final TokenHeader header) throws Exception {
    if (header == null) {
      throw new IllegalArgumentException("Token header is null");
    }

    EntityImpl entity = new EntityImpl();
    entity.field("typ", header.getType());
    entity.field("alg", header.getAlgorithm());
    entity.field("kid", header.getKeyId());
    return entity.toJSON().getBytes(StandardCharsets.UTF_8);
  }

  protected byte[] serializeWebPayload(final JwtPayload payload) throws Exception {
    if (payload == null) {
      throw new IllegalArgumentException("Token payload is null");
    }

    final EntityImpl entity = new EntityImpl();
    entity.field("username", payload.getUserName());
    entity.field("iss", payload.getIssuer());
    entity.field("exp", payload.getExpiry());
    entity.field("iat", payload.getIssuedAt());
    entity.field("nbf", payload.getNotBefore());
    entity.field("sub", payload.getDatabase());
    entity.field("aud", payload.getAudience());
    entity.field("jti", payload.getTokenId());
    entity.field("uidc", payload.getUserRid().getClusterId());
    entity.field("uidp", payload.getUserRid().getClusterPosition());
    entity.field("bdtyp", payload.getDatabaseType());
    return entity.toJSON().getBytes(StandardCharsets.UTF_8);
  }

  protected JwtPayload createPayloadServerUser(SecurityUser serverUser) {
    if (serverUser == null) {
      throw new IllegalArgumentException("User is null");
    }

    final YouTrackDBJwtPayload payload = new YouTrackDBJwtPayload();
    payload.setAudience("YouTrackDBServer");
    payload.setDatabase("-");
    payload.setUserRid(ImmutableRecordId.EMPTY_RECORD_ID);

    final long expiryMinutes = sessionInMills;
    final long currTime = System.currentTimeMillis();
    payload.setIssuedAt(currTime);
    payload.setNotBefore(currTime);
    payload.setUserName(serverUser.getName(null));
    payload.setTokenId(UUID.randomUUID().toString());
    payload.setExpiry(currTime + expiryMinutes);
    return payload;
  }

  protected JwtPayload createPayload(final DatabaseSessionInternal db,
      final SecurityUser user) {
    if (user == null) {
      throw new IllegalArgumentException("User is null");
    }

    final YouTrackDBJwtPayload payload = new YouTrackDBJwtPayload();
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

  protected String getPayloadType(final JwtPayload payload) {
    return "YouTrackDB";
  }

  private BinaryToken deserializeBinaryToken(final InputStream bais) {
    try {
      return binarySerializer.deserialize(bais);
    } catch (Exception e) {
      throw BaseException.wrapException(new SystemException("Cannot deserialize binary token"), e);
    }
  }

  public void setSessionInMills(long sessionInMills) {
    this.sessionInMills = sessionInMills;
  }
}
