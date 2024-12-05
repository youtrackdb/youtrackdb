package com.jetbrains.youtrack.db.internal.core.security.authenticator;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTSecurityAccessException;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurityShared;
import com.jetbrains.youtrack.db.internal.core.metadata.security.YTSecurityUser;
import com.jetbrains.youtrack.db.internal.core.metadata.security.YTUser;
import com.jetbrains.youtrack.db.internal.core.metadata.security.auth.OAuthenticationInfo;
import com.jetbrains.youtrack.db.internal.core.metadata.security.auth.OTokenAuthInfo;
import com.jetbrains.youtrack.db.internal.core.metadata.security.auth.OUserPasswordAuthInfo;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.security.OParsedToken;
import com.jetbrains.youtrack.db.internal.core.security.OSecuritySystem;
import com.jetbrains.youtrack.db.internal.core.security.OTokenSign;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.YTTokenSecurityException;

public class ODatabaseUserAuthenticator extends OSecurityAuthenticatorAbstract {

  private OTokenSign tokenSign;

  @Override
  public void config(YTDatabaseSessionInternal session, EntityImpl jsonConfig,
      OSecuritySystem security) {
    super.config(session, jsonConfig, security);
    tokenSign = security.getTokenSign();
  }

  @Override
  public YTSecurityUser authenticate(YTDatabaseSessionInternal session, OAuthenticationInfo info) {
    if (info instanceof OUserPasswordAuthInfo) {
      return authenticate(
          session,
          ((OUserPasswordAuthInfo) info).getUser(),
          ((OUserPasswordAuthInfo) info).getPassword());
    } else if (info instanceof OTokenAuthInfo) {
      OParsedToken token = ((OTokenAuthInfo) info).getToken();

      if (tokenSign != null && !tokenSign.verifyTokenSign(token)) {
        throw new YTTokenSecurityException("The token provided is expired");
      }
      if (!token.getToken().getIsValid()) {
        throw new YTSecurityAccessException(session.getName(), "Token not valid");
      }

      YTUser user = token.getToken().getUser(session);
      if (user == null && token.getToken().getUserName() != null) {
        OSecurityShared databaseSecurity =
            (OSecurityShared) session.getSharedContext().getSecurity();
        user = OSecurityShared.getUserInternal(session, token.getToken().getUserName());
      }
      return user;
    }
    return super.authenticate(session, info);
  }

  @Override
  public YTSecurityUser authenticate(YTDatabaseSessionInternal session, String username,
      String password) {
    if (session == null) {
      return null;
    }

    String dbName = session.getName();
    YTUser user = OSecurityShared.getUserInternal(session, username);
    if (user == null) {
      return null;
    }
    if (user.getAccountStatus(session) != YTSecurityUser.STATUSES.ACTIVE) {
      throw new YTSecurityAccessException(dbName, "User '" + username + "' is not active");
    }

    // CHECK USER & PASSWORD
    if (!user.checkPassword(session, password)) {
      // WAIT A BIT TO AVOID BRUTE FORCE
      try {
        Thread.sleep(200);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
      throw new YTSecurityAccessException(
          dbName, "User or password not valid for database: '" + dbName + "'");
    }

    return user;
  }

  @Override
  public YTSecurityUser getUser(String username, YTDatabaseSessionInternal session) {
    return null;
  }

  @Override
  public boolean isAuthorized(YTDatabaseSessionInternal session, String username, String resource) {
    return false;
  }

  @Override
  public boolean isSingleSignOnSupported() {
    return false;
  }
}
