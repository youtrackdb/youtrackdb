package com.orientechnologies.core.metadata.security;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.metadata.security.jwt.OTokenHeader;

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

  YTUser getUser(YTDatabaseSessionInternal db);

  String getDatabase();

  String getDatabaseType();

  YTRID getUserId();

  long getExpiry();

  void setExpiry(long expiry);

  boolean isNowValid();

  boolean isCloseToExpire();
}
