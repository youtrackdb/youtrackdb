package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.security.jwt.OTokenHeader;

/**
 *
 */
public interface OToken {

  OTokenHeader getHeader();

  boolean getIsVerified();

  void setIsVerified(boolean verified);

  boolean getIsValid();

  void setIsValid(boolean valid);

  String getUserName();

  OUser getUser(ODatabaseSessionInternal db);

  String getDatabase();

  String getDatabaseType();

  ORID getUserId();

  long getExpiry();

  void setExpiry(long expiry);

  boolean isNowValid();

  boolean isCloseToExpire();
}
