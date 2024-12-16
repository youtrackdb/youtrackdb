package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.metadata.security.jwt.TokenHeader;

/**
 *
 */
public interface Token {

  TokenHeader getHeader();

  boolean getIsVerified();

  void setIsVerified(boolean verified);

  boolean getIsValid();

  void setIsValid(boolean valid);

  String getUserName();

  SecurityUserIml getUser(DatabaseSessionInternal db);

  String getDatabase();

  String getDatabaseType();

  RID getUserId();

  long getExpiry();

  void setExpiry(long expiry);

  boolean isNowValid();

  boolean isCloseToExpire();
}
