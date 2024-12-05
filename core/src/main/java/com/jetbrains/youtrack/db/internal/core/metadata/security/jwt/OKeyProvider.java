package com.jetbrains.youtrack.db.internal.core.metadata.security.jwt;

import java.security.Key;

/**
 *
 */
public interface OKeyProvider {

  Key getKey(OTokenHeader header);

  String[] getKeys();

  String getDefaultKey();
}
