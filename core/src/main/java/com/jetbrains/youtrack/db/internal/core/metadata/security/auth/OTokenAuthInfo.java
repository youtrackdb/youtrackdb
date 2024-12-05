package com.jetbrains.youtrack.db.internal.core.metadata.security.auth;

import com.jetbrains.youtrack.db.internal.core.security.OParsedToken;
import java.util.Optional;

public class OTokenAuthInfo implements OAuthenticationInfo {

  private final OParsedToken token;

  public OTokenAuthInfo(OParsedToken iToken) {
    this.token = iToken;
  }

  @Override
  public Optional<String> getDatabase() {
    return Optional.ofNullable(token.getToken().getDatabase());
  }

  public OParsedToken getToken() {
    return token;
  }
}
