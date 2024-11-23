package com.orientechnologies.orient.core.metadata.security.jwt;

import java.security.Key;

/**
 *
 */
public interface OKeyProvider {

  Key getKey(OTokenHeader header);

  String[] getKeys();

  String getDefaultKey();
}
