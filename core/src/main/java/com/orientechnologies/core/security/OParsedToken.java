package com.orientechnologies.core.security;

import com.orientechnologies.core.metadata.security.OToken;

public class OParsedToken {

  private final OToken token;
  private final byte[] tokenBytes;
  private final byte[] signature;

  public OParsedToken(OToken token, byte[] tokenBytes, byte[] signature) {
    super();
    this.token = token;
    this.tokenBytes = tokenBytes;
    this.signature = signature;
  }

  public OToken getToken() {
    return token;
  }

  public byte[] getTokenBytes() {
    return tokenBytes;
  }

  public byte[] getSignature() {
    return signature;
  }
}
