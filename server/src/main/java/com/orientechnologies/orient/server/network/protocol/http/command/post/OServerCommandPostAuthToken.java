package com.orientechnologies.orient.server.network.protocol.http.command.post;

import com.orientechnologies.common.concur.lock.YTLockException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.core.config.YTGlobalConfiguration;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.exception.YTSecurityAccessException;
import com.orientechnologies.core.metadata.security.YTSecurityUser;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.server.OTokenHandler;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAbstract;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 *
 */
public class OServerCommandPostAuthToken extends OServerCommandAbstract {

  private static final String[] NAMES = {"POST|token/*"};
  private static final String RESPONSE_FORMAT = "indent:-1,attribSameRow";
  private volatile OTokenHandler tokenHandler;

  @Override
  public String[] getNames() {
    return NAMES;
  }

  private void init() {

    if (tokenHandler == null
        && server
        .getContextConfiguration()
        .getValueAsBoolean(YTGlobalConfiguration.NETWORK_HTTP_USE_TOKEN)) {
      tokenHandler = server.getTokenHandler();
    }
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    init();
    String[] urlParts = checkSyntax(iRequest.getUrl(), 2, "Syntax error: token/<database>");
    iRequest.setDatabaseName(urlParts[1]);

    iRequest.getData().commandInfo = "Generate authentication token";

    // Parameter names consistent with 4.3.2 (Access Token Request) of RFC 6749
    Map<String, String> content = iRequest.getUrlEncodedContent();
    if (content == null) {
      YTEntityImpl result = new YTEntityImpl().field("error", "missing_auth_data");
      sendError(iRequest, iResponse, result);
      return false;
    }
    String signedToken = ""; // signedJWT.serialize();

    String grantType = content.get("grant_type").toLowerCase(Locale.ENGLISH);
    String username = content.get("username");
    String password = content.get("password");
    String authenticatedRid;
    YTEntityImpl result;

    if (grantType.equals("password")) {
      authenticatedRid = authenticate(username, password, iRequest.getDatabaseName());
      if (authenticatedRid == null) {
        sendAuthorizationRequest(iRequest, iResponse, iRequest.getDatabaseName());
      } else if (tokenHandler != null) {
        // Generate and return a JWT access token

        YTSecurityUser user = null;
        try (YTDatabaseSessionInternal db = server.openDatabase(iRequest.getDatabaseName(),
            username,
            password)) {
          user = db.getUser();

          if (user != null) {
            byte[] tokenBytes = tokenHandler.getSignedWebToken(db, user);
            signedToken = new String(tokenBytes);
          }

        } catch (YTSecurityAccessException e) {
          // WRONG USER/PASSWD
        } catch (YTLockException e) {
          OLogManager.instance()
              .error(this, "Cannot access to the database '" + iRequest.getDatabaseName() + "'", e);
        }

        // 4.1.4 (Access Token Response) of RFC 6749
        result = new YTEntityImpl().field("access_token", signedToken).field("expires_in", 3600);

        iResponse.writeRecord(result, RESPONSE_FORMAT, null);
      } else {
        result = new YTEntityImpl().field("error", "unsupported_grant_type");
        sendError(iRequest, iResponse, result);
      }
    } else {
      result = new YTEntityImpl().field("error", "unsupported_grant_type");
      sendError(iRequest, iResponse, result);
    }

    return false;
  }

  // Return user rid if authentication successful.
  // If user is server user (doesn't have a rid) then '<server user>' is returned.
  // null is returned in all other cases and means authentication was unsuccessful.
  protected String authenticate(
      final String username, final String password, final String iDatabaseName) throws IOException {
    YTDatabaseSessionInternal db = null;
    String userRid = null;
    try {
      db = server.openDatabase(iDatabaseName, username, password);

      userRid = (db.getUser() == null ? "<server user>" : db.getUser().getIdentity(db).toString());
    } catch (YTSecurityAccessException e) {
      // WRONG USER/PASSWD
    } catch (YTLockException e) {
      OLogManager.instance()
          .error(this, "Cannot access to the database '" + iDatabaseName + "'", e);
    } finally {
      if (db != null) {
        db.close();
      }
    }
    return userRid;
  }

  protected void sendError(
      final OHttpRequest iRequest, final OHttpResponse iResponse, final YTEntityImpl error)
      throws IOException {
    iResponse.send(
        OHttpUtils.STATUS_BADREQ_CODE,
        OHttpUtils.STATUS_BADREQ_DESCRIPTION,
        OHttpUtils.CONTENT_JSON,
        error.toJSON(),
        null);
  }

  protected void sendAuthorizationRequest(
      final OHttpRequest iRequest, final OHttpResponse iResponse, final String iDatabaseName)
      throws IOException {

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
          OHttpUtils.STATUS_BADREQ_CODE,
          OHttpUtils.STATUS_BADREQ_DESCRIPTION,
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
}
