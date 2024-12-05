package com.orientechnologies.core.metadata.security.jwt;

public interface OTokenMetaInfo {

  String getDbType(int pos);

  int getDbTypeID(String databaseType);
}
