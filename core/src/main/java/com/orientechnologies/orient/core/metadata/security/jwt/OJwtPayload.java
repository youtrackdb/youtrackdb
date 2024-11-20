package com.orientechnologies.orient.core.metadata.security.jwt;

/**
 * Created by emrul on 28/09/2014.
 *
 * @author Emrul Islam <emrul@emrul.com> Copyright 2014 Emrul Islam
 */
public interface OJwtPayload extends OTokenPayload {

  String getIssuer();

  void setIssuer(String iss);

  long getIssuedAt();

  void setIssuedAt(long iat);

  long getNotBefore();

  void setNotBefore(long nbf);

  void setUserName(String sub);

  String getAudience();

  void setAudience(String aud);

  String getTokenId();

  void setTokenId(String jti);

  void setDatabase(String database);

  void setDatabaseType(String databaseType);
}
