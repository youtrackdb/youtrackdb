package com.jetbrains.youtrack.db.internal.core.security;

public interface SecurityConfig {

  Syslog getSyslog();

  String getConfigurationFile();
}
