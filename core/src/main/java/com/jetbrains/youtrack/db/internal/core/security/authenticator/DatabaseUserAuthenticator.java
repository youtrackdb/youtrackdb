package com.jetbrains.youtrack.db.internal.core.security.authenticator;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.exception.SecurityAccessException;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityShared;
import com.jetbrains.youtrack.db.api.security.SecurityUser;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityUserIml;
import com.jetbrains.youtrack.db.internal.core.metadata.security.auth.AuthenticationInfo;
import com.jetbrains.youtrack.db.internal.core.metadata.security.auth.TokenAuthInfo;
import com.jetbrains.youtrack.db.internal.core.metadata.security.auth.UserPasswordAuthInfo;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.security.ParsedToken;
import com.jetbrains.youtrack.db.internal.core.security.SecuritySystem;
import com.jetbrains.youtrack.db.internal.core.security.TokenSign;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.TokenSecurityException;

public class DatabaseUserAuthenticator extends SecurityAuthenticatorAbstract {

  private TokenSign tokenSign;

  @Override
  public void config(DatabaseSessionInternal session, EntityImpl jsonConfig,
      SecuritySystem security) {
    super.config(session, jsonConfig, security);
    tokenSign = security.getTokenSign();
  }

  @Override
  public SecurityUser authenticate(DatabaseSessionInternal session, AuthenticationInfo info) {
    if (info instanceof UserPasswordAuthInfo) {
      return authenticate(
          session,
          ((UserPasswordAuthInfo) info).getUser(),
          ((UserPasswordAuthInfo) info).getPassword());
    } else if (info instanceof TokenAuthInfo) {
      ParsedToken token = ((TokenAuthInfo) info).getToken();

      if (tokenSign != null && !tokenSign.verifyTokenSign(token)) {
        throw new TokenSecurityException("The token provided is expired");
      }
      if (!token.getToken().getIsValid()) {
        throw new SecurityAccessException(session.getName(), "Token not valid");
      }

      SecurityUserIml user = token.getToken().getUser(session);
      if (user == null && token.getToken().getUserName() != null) {
        SecurityShared databaseSecurity =
            (SecurityShared) session.getSharedContext().getSecurity();
        user = SecurityShared.getUserInternal(session, token.getToken().getUserName());
      }
      return user;
    }
    return super.authenticate(session, info);
  }

  @Override
  public SecurityUser authenticate(DatabaseSessionInternal session, String username,
      String password) {
    if (session == null) {
      return null;
    }

    String dbName = session.getName();
    SecurityUserIml user = SecurityShared.getUserInternal(session, username);
    if (user == null) {
      return null;
    }
    if (user.getAccountStatus(session) != SecurityUser.STATUSES.ACTIVE) {
      throw new SecurityAccessException(dbName, "User '" + username + "' is not active");
    }

    // CHECK USER & PASSWORD
    if (!user.checkPassword(session, password)) {
      // WAIT A BIT TO AVOID BRUTE FORCE
      try {
        Thread.sleep(200);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
      throw new SecurityAccessException(
          dbName, "User or password not valid for database: '" + dbName + "'");
    }

    return user;
  }

  @Override
  public SecurityUser getUser(String username, DatabaseSessionInternal session) {
    return null;
  }

  @Override
  public boolean isAuthorized(DatabaseSessionInternal session, String username, String resource) {
    return false;
  }

  @Override
  public boolean isSingleSignOnSupported() {
    return false;
  }
}
