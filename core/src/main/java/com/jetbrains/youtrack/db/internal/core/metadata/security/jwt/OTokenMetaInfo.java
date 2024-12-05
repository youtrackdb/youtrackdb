package com.jetbrains.youtrack.db.internal.core.metadata.security.jwt;

public interface OTokenMetaInfo {

  String getDbType(int pos);

  int getDbTypeID(String databaseType);
}
