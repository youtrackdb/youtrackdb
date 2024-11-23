package com.orientechnologies.orient.core.metadata.security.jwt;

/**
 *
 */
public interface OTokenHeader {

  String getAlgorithm();

  void setAlgorithm(String alg);

  String getType();

  void setType(String typ);

  String getKeyId();

  void setKeyId(String kid);
}
