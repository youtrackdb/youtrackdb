package com.orientechnologies.orient.core.metadata.security.auth;

import java.util.Optional;

public interface OAuthenticationInfo {

  Optional<String> getDatabase();
}
