package com.jetbrains.youtrack.db.internal.core.metadata.security.jwt;

import com.jetbrains.youtrack.db.api.record.RID;
import java.io.DataOutputStream;
import java.io.IOException;

public interface TokenPayload {

  String getDatabase();

  long getExpiry();

  RID getUserRid();

  String getDatabaseType();

  String getUserName();

  void setExpiry(long expiry);

  String getPayloadType();

  void serialize(DataOutputStream output, TokenMetaInfo serializer) throws IOException;
}
