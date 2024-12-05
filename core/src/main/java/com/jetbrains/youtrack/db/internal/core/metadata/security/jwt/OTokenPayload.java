package com.jetbrains.youtrack.db.internal.core.metadata.security.jwt;

import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import java.io.DataOutputStream;
import java.io.IOException;

public interface OTokenPayload {

  String getDatabase();

  long getExpiry();

  YTRID getUserRid();

  String getDatabaseType();

  String getUserName();

  void setExpiry(long expiry);

  String getPayloadType();

  void serialize(DataOutputStream output, OTokenMetaInfo serializer) throws IOException;
}
