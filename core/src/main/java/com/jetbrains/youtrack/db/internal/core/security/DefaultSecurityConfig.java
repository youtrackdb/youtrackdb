package com.jetbrains.youtrack.db.internal.core.security;

public class DefaultSecurityConfig implements SecurityConfig {

  @Override
  public Syslog getSyslog() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getConfigurationFile() {
    return null;
  }
}
