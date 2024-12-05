package com.jetbrains.youtrack.db.internal.core.security;

public class ODefaultSecurityConfig implements OSecurityConfig {

  @Override
  public OSyslog getSyslog() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getConfigurationFile() {
    return null;
  }
}
