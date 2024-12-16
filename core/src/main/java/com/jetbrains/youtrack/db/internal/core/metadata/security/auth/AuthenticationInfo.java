package com.jetbrains.youtrack.db.internal.core.metadata.security.auth;

import java.util.Optional;

public interface AuthenticationInfo {

  Optional<String> getDatabase();
}
