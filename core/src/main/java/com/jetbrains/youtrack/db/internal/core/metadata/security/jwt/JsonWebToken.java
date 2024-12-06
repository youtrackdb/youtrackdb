package com.jetbrains.youtrack.db.internal.core.metadata.security.jwt;

/**
 *
 */
public interface JsonWebToken {

  TokenHeader getHeader();

  JwtPayload getPayload();
}
