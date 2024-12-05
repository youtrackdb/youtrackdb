package com.jetbrains.youtrack.db.internal.core.security;

import com.jetbrains.youtrack.db.internal.core.metadata.security.jwt.OTokenHeader;

public interface OTokenSign {

  byte[] signToken(OTokenHeader header, byte[] unsignedToken);

  boolean verifyTokenSign(OParsedToken parsed);

  String getAlgorithm();

  String getDefaultKey();

  String[] getKeys();
}
