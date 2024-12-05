package com.jetbrains.youtrack.db.internal.core.metadata.security.jwt;

/**
 *
 */
public interface OJsonWebToken {

  OTokenHeader getHeader();

  OJwtPayload getPayload();
}
