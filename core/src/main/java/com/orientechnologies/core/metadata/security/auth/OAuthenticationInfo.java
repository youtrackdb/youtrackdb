package com.orientechnologies.core.metadata.security.auth;

import java.util.Optional;

public interface OAuthenticationInfo {

  Optional<String> getDatabase();
}
