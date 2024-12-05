package com.jetbrains.youtrack.db.internal.core.metadata.security.jwt;

public interface OBinaryTokenPayload extends OTokenPayload {

  short getProtocolVersion();

  String getSerializer();

  String getDriverName();

  String getDriverVersion();

  boolean isServerUser();
}
