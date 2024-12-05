/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.orientechnologies.orient.server.network.protocol.http.command;

import com.orientechnologies.common.concur.lock.YTLockException;
import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.exception.YTSecurityAccessException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.metadata.security.YTUser;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.server.OTokenHandler;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpSession;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.YTHttpRequestException;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Database based authenticated command. Authenticates against the database taken as second
 * parameter of the URL. The URL must be in this format:
 *
 * <p>
 *
 * <pre>
 * <command>/<database>[/...]
 * </pre>
 */
public abstract class OServerCommandAuthenticatedDbAbstract extends OServerCommandAbstract {

  public static final char DBNAME_DIR_SEPARATOR = '$';
  public static final String SESSIONID_UNAUTHORIZED = "-";
  public static final String SESSIONID_LOGOUT = "!";
  private volatile OTokenHandler tokenHandler;

  @Override
  public boolean beforeExecute(final OHttpRequest iRequest, OHttpResponse iResponse)
      throws IOException {
    super.beforeExecute(iRequest, iResponse);

    try {
      init();

      final String[] urlParts = iRequest.getUrl().substring(1).split("/");
      if (urlParts.length < 2) {
        throw new YTHttpRequestException(
            "Syntax error in URL. Expected is: <command>/<database>[/...]");
      }

      iRequest.setDatabaseName(URLDecoder.decode(urlParts[1], StandardCharsets.UTF_8));
      if (iRequest.getBearerTokenRaw() != null) {
        // Bearer authentication
        try {
          iRequest.setBearerToken(
              tokenHandler.parseOnlyWebToken(iRequest.getBearerTokenRaw().getBytes()));
        } catch (Exception e) {
          // TODO: Catch all expected exceptions correctly!
          OLogManager.instance().warn(this, "Bearer token parsing failed", e);
        }

        if (iRequest.getBearerToken() == null
            || !iRequest.getBearerToken().getToken().getIsVerified()) {
          // Token parsing or verification failed - for now fail silently.
          sendAuthorizationRequest(iRequest, iResponse, iRequest.getDatabaseName());
          return false;
        }

        // CHECK THE REQUEST VALIDITY
        tokenHandler.validateToken(iRequest.getBearerToken(), urlParts[0], urlParts[1]);
        if (!iRequest.getBearerToken().getToken().getIsValid()) {

          // SECURITY PROBLEM: CROSS DATABASE REQUEST!
          OLogManager.instance()
              .warn(
                  this,
                  "Token '%s' is not valid for database '%s'",
                  iRequest.getBearerTokenRaw(),
                  iRequest.getDatabaseName());
          sendAuthorizationRequest(iRequest, iResponse, iRequest.getDatabaseName());
          return false;
        }

        return iRequest.getBearerToken().getToken().getIsValid();
      } else {
        // HTTP basic authentication
        final List<String> authenticationParts =
            iRequest.getAuthorization() != null
                ? OStringSerializerHelper.split(iRequest.getAuthorization(), ':')
                : null;

        OHttpSession currentSession;
        if (iRequest.getSessionId() != null && iRequest.getSessionId().length() > 1) {
          currentSession = server.getHttpSessionManager().getSession(iRequest.getSessionId());
          if (currentSession != null && authenticationParts != null) {
            if (!currentSession.getUserName().equals(authenticationParts.get(0))) {
              // CHANGED USER, INVALIDATE THE SESSION
              currentSession = null;
            }
          }
        } else {
          currentSession = null;
        }

        if (currentSession == null) {
          // NO SESSION
          if (iRequest.getAuthorization() == null
              || SESSIONID_LOGOUT.equals(iRequest.getSessionId())) {
            iResponse.setSessionId(SESSIONID_UNAUTHORIZED);
            sendAuthorizationRequest(iRequest, iResponse, iRequest.getDatabaseName());
            return false;
          } else {
            return authenticate(
                iRequest, iResponse, authenticationParts, iRequest.getDatabaseName());
          }

        } else {
          // CHECK THE SESSION VALIDITY
          if (!currentSession.getDatabaseName().equals(iRequest.getDatabaseName())) {

            // SECURITY PROBLEM: CROSS DATABASE REQUEST!
            OLogManager.instance()
                .warn(
                    this,
                    "Session %s is trying to access to the database '%s', but has been"
                        + " authenticated against the database '%s'",
                    iRequest.getSessionId(),
                    iRequest.getDatabaseName(),
                    currentSession.getDatabaseName());
            server.getHttpSessionManager().removeSession(iRequest.getSessionId());
            sendAuthorizationRequest(iRequest, iResponse, iRequest.getDatabaseName());
            return false;

          } else if (authenticationParts != null
              && !currentSession.getUserName().equals(authenticationParts.get(0))) {

            // SECURITY PROBLEM: CROSS DATABASE REQUEST!
            OLogManager.instance()
                .warn(
                    this,
                    "Session %s is trying to access to the database '%s' with user '%s', but has"
                        + " been authenticated with user '%s'",
                    iRequest.getSessionId(),
                    iRequest.getDatabaseName(),
                    authenticationParts.get(0),
                    currentSession.getUserName());
            server.getHttpSessionManager().removeSession(iRequest.getSessionId());
            sendAuthorizationRequest(iRequest, iResponse, iRequest.getDatabaseName());
            return false;
          }

          return true;
        }
      }
    } catch (Exception e) {
      throw YTException.wrapException(new YTHttpRequestException("Error on authentication"), e);
    } finally {
      // clear local cache to ensure that zomby records will not pile up in cache.
      try {
        if (iRequest.getDatabaseName() != null) {
          YTDatabaseSessionInternal db = getProfiledDatabaseInstance(iRequest);
          if (db != null && !db.getTransaction().isActive()) {
            db.activateOnCurrentThread();
            db.getLocalCache().clear();
          }
        }
      } catch (Exception e) {
        // ignore
      }
    }
  }

  @Override
  public boolean afterExecute(final OHttpRequest iRequest, OHttpResponse iResponse)
      throws IOException {
    ODatabaseRecordThreadLocal.instance().remove();
    iRequest.getExecutor().setDatabase(null);
    return true;
  }

  protected boolean authenticate(
      final OHttpRequest iRequest,
      final OHttpResponse iResponse,
      final List<String> iAuthenticationParts,
      final String iDatabaseName)
      throws IOException {
    YTDatabaseSessionInternal db = null;
    try {
      db =
          server.openDatabase(
              iDatabaseName, iAuthenticationParts.get(0), iAuthenticationParts.get(1));
      // if (db.getUser() == null)
      // // MAYBE A PREVIOUS ROOT REALM? UN AUTHORIZE
      // return false;

      // Set user rid after authentication
      iRequest.getData().currentUserId =
          db.getUser() == null ? "<server user>" : db.getUser().getIdentity(db).toString();

      // AUTHENTICATED: CREATE THE SESSION
      iRequest.setSessionId(
          server
              .getHttpSessionManager()
              .createSession(
                  iDatabaseName, iAuthenticationParts.get(0), iAuthenticationParts.get(1)));
      iResponse.setSessionId(iRequest.getSessionId());
      return true;

    } catch (YTSecurityAccessException e) {
      // WRONG USER/PASSWD
    } catch (YTLockException e) {
      OLogManager.instance()
          .error(this, "Cannot access to the database '" + iDatabaseName + "'", e);
    } finally {
      if (db == null) {
        // WRONG USER/PASSWD
        sendAuthorizationRequest(iRequest, iResponse, iDatabaseName);
      } else {
        db.close();
      }
    }
    return false;
  }

  protected void sendAuthorizationRequest(
      final OHttpRequest iRequest, final OHttpResponse iResponse, final String iDatabaseName)
      throws IOException {
    // UNAUTHORIZED
    iRequest.setSessionId(SESSIONID_UNAUTHORIZED);

    String header = null;
    String xRequestedWithHeader = iRequest.getHeader("X-Requested-With");
    if (xRequestedWithHeader == null || !xRequestedWithHeader.equals("XMLHttpRequest")) {
      // Defaults to "WWW-Authenticate: Basic" if not an AJAX Request.
      header = server.getSecurity().getAuthenticationHeader(iDatabaseName);

      Map<String, String> headers = server.getSecurity().getAuthenticationHeaders(iDatabaseName);
      headers.entrySet().forEach(s -> iResponse.addHeader(s.getKey(), s.getValue()));
    }

    if (isJsonResponse(iResponse)) {
      sendJsonError(
          iResponse,
          OHttpUtils.STATUS_AUTH_CODE,
          OHttpUtils.STATUS_AUTH_DESCRIPTION,
          OHttpUtils.CONTENT_TEXT_PLAIN,
          "401 Unauthorized.",
          header);
    } else {
      iResponse.send(
          OHttpUtils.STATUS_AUTH_CODE,
          OHttpUtils.STATUS_AUTH_DESCRIPTION,
          OHttpUtils.CONTENT_TEXT_PLAIN,
          "401 Unauthorized.",
          header);
    }
  }

  protected YTDatabaseSessionInternal getProfiledDatabaseInstance(final OHttpRequest iRequest)
      throws InterruptedException {
    if (iRequest.getBearerToken() != null) {
      return getProfiledDatabaseInstanceToken(iRequest);
    } else {
      return getProfiledDatabaseInstanceBasic(iRequest);
    }
  }

  protected YTDatabaseSessionInternal getProfiledDatabaseInstanceToken(final OHttpRequest iRequest)
      throws InterruptedException {
    // after authentication, if current login user is different compare with current DB user, reset
    // DB user to login user
    YTDatabaseSessionInternal localDatabase = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (localDatabase == null) {
      localDatabase = server.openDatabase(iRequest.getDatabaseName(), iRequest.getBearerToken());
    } else {
      YTRID currentUserId = iRequest.getBearerToken().getToken().getUserId();
      if (currentUserId != null && localDatabase.getUser() != null) {
        if (!currentUserId.equals(
            localDatabase.getUser().getIdentity(localDatabase).getIdentity())) {
          YTEntityImpl userDoc = localDatabase.load(currentUserId);
          localDatabase.setUser(new YTUser(localDatabase, userDoc));
        }
      }
    }

    iRequest.getData().lastDatabase = localDatabase.getName();
    iRequest.getData().lastUser =
        localDatabase.getUser() != null ? localDatabase.getUser().getName(localDatabase) : null;
    return localDatabase.getDatabaseOwner();
  }

  protected YTDatabaseSessionInternal getProfiledDatabaseInstanceBasic(
      final OHttpRequest iRequest) {
    final OHttpSession session = server.getHttpSessionManager().getSession(iRequest.getSessionId());

    if (session == null) {
      throw new YTSecurityAccessException(iRequest.getDatabaseName(), "No session active");
    }

    // after authentication, if current login user is different compare with current DB user, reset
    // DB user to login user
    YTDatabaseSessionInternal localDatabase = ODatabaseRecordThreadLocal.instance().getIfDefined();

    if (localDatabase == null) {
      localDatabase =
          server.openDatabase(
              iRequest.getDatabaseName(), session.getUserName(), session.getUserPassword());
    } else {
      String currentUserId = iRequest.getData().currentUserId;
      if (currentUserId != null && !currentUserId.isEmpty() && localDatabase.getUser() != null) {
        if (!currentUserId.equals(localDatabase.getUser().getIdentity(localDatabase).toString())) {
          YTEntityImpl userDoc = localDatabase.load(new YTRecordId(currentUserId));
          localDatabase.setUser(new YTUser(localDatabase, userDoc));
        }
      }
    }

    iRequest.getData().lastDatabase = localDatabase.getName();
    iRequest.getData().lastUser =
        localDatabase.getUser() != null ? localDatabase.getUser().getName(localDatabase) : null;
    iRequest.getExecutor().setDatabase(localDatabase);
    return localDatabase.getDatabaseOwner();
  }

  private void init() {
    if (tokenHandler == null
        && server
        .getContextConfiguration()
        .getValueAsBoolean(YTGlobalConfiguration.NETWORK_HTTP_USE_TOKEN)) {
      tokenHandler = server.getTokenHandler();
    }
  }
}
