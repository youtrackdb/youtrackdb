package com.jetbrains.youtrack.db.internal.core.security;

public interface OSecurityConfig {

  OSyslog getSyslog();

  String getConfigurationFile();
}
