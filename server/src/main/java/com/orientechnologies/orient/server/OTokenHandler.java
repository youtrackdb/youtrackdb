package com.orientechnologies.orient.server;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.metadata.security.OToken;
import com.orientechnologies.core.metadata.security.YTSecurityUser;
import com.orientechnologies.core.security.OParsedToken;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 *
 */
public interface OTokenHandler {

  @Deprecated
  String TOKEN_HANDLER_NAME = "OTokenHandler";

  // Return null if token is unparseable or fails verification.
  // The returned token should be checked to ensure isVerified == true.
  OToken parseWebToken(byte[] tokenBytes)
      throws InvalidKeyException, NoSuchAlgorithmException, IOException;

  OParsedToken parseOnlyWebToken(byte[] tokenBytes);

  OToken parseNotVerifyBinaryToken(byte[] tokenBytes);

  OToken parseBinaryToken(byte[] tokenBytes);

  OParsedToken parseOnlyBinary(byte[] tokenBytes);

  boolean validateToken(OToken token, String command, String database);

  boolean validateToken(OParsedToken token, String command, String database);

  boolean validateBinaryToken(OToken token);

  boolean validateBinaryToken(OParsedToken token);

  ONetworkProtocolData getProtocolDataFromToken(OClientConnection oClientConnection, OToken token);

  // Return a byte array representing a signed token
  byte[] getSignedWebToken(YTDatabaseSessionInternal db, YTSecurityUser user);

  default byte[] getSignedWebTokenServerUser(YTSecurityUser user) {
    throw new UnsupportedOperationException();
  }

  default boolean validateServerUserToken(OToken token, String command, String database) {
    throw new UnsupportedOperationException();
  }

  byte[] getSignedBinaryToken(
      YTDatabaseSessionInternal db, YTSecurityUser user, ONetworkProtocolData data);

  byte[] renewIfNeeded(OToken token);

  boolean isEnabled();
}
