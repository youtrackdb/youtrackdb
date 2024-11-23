package com.orientechnologies.orient.core.metadata.security.jwt;

/**
 *
 */
public interface OJsonWebToken {

  OTokenHeader getHeader();

  OJwtPayload getPayload();
}
