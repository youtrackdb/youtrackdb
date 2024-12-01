package com.orientechnologies.orient.core.security.authenticator;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.metadata.security.auth.OAuthenticationInfo;
import com.orientechnologies.orient.core.metadata.security.auth.OTokenAuthInfo;
import com.orientechnologies.orient.core.metadata.security.auth.OUserPasswordAuthInfo;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OParsedToken;
import com.orientechnologies.orient.core.security.OSecuritySystem;
import com.orientechnologies.orient.core.security.OTokenSign;
import com.orientechnologies.orient.enterprise.channel.binary.OTokenSecurityException;

public class ODatabaseUserAuthenticator extends OSecurityAuthenticatorAbstract {

  private OTokenSign tokenSign;

  @Override
  public void config(ODatabaseSessionInternal session, ODocument jsonConfig,
      OSecuritySystem security) {
    super.config(session, jsonConfig, security);
    tokenSign = security.getTokenSign();
  }

  @Override
  public OSecurityUser authenticate(ODatabaseSessionInternal session, OAuthenticationInfo info) {
    if (info instanceof OUserPasswordAuthInfo) {
      return authenticate(
          session,
          ((OUserPasswordAuthInfo) info).getUser(),
          ((OUserPasswordAuthInfo) info).getPassword());
    } else if (info instanceof OTokenAuthInfo) {
      OParsedToken token = ((OTokenAuthInfo) info).getToken();

      if (tokenSign != null && !tokenSign.verifyTokenSign(token)) {
        throw new OTokenSecurityException("The token provided is expired");
      }
      if (!token.getToken().getIsValid()) {
        throw new OSecurityAccessException(session.getName(), "Token not valid");
      }

      OUser user = token.getToken().getUser(session);
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
  public OSecurityUser authenticate(ODatabaseSessionInternal session, String username,
      String password) {
    if (session == null) {
      return null;
    }

    String dbName = session.getName();
    OUser user = OSecurityShared.getUserInternal(session, username);
    if (user == null) {
      return null;
    }
    if (user.getAccountStatus(session) != OSecurityUser.STATUSES.ACTIVE) {
      throw new OSecurityAccessException(dbName, "User '" + username + "' is not active");
    }

    // CHECK USER & PASSWORD
    if (!user.checkPassword(session, password)) {
      // WAIT A BIT TO AVOID BRUTE FORCE
      try {
        Thread.sleep(200);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
      throw new OSecurityAccessException(
          dbName, "User or password not valid for database: '" + dbName + "'");
    }

    return user;
  }

  @Override
  public OSecurityUser getUser(String username, ODatabaseSessionInternal session) {
    return null;
  }

  @Override
  public boolean isAuthorized(ODatabaseSessionInternal session, String username, String resource) {
    return false;
  }

  @Override
  public boolean isSingleSignOnSupported() {
    return false;
  }
}
