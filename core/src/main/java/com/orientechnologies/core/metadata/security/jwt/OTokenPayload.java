package com.orientechnologies.core.metadata.security.jwt;

import com.orientechnologies.core.id.YTRID;
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
