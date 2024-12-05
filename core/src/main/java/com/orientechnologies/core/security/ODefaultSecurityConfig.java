package com.orientechnologies.core.security;

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
