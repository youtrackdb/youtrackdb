package com.orientechnologies.core.metadata.security.jwt;

/**
 *
 */
public interface OJsonWebToken {

  OTokenHeader getHeader();

  OJwtPayload getPayload();
}
