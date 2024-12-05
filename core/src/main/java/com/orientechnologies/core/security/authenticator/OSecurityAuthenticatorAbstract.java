/*
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
package com.orientechnologies.core.security.authenticator;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.metadata.security.YTSecurityUser;
import com.orientechnologies.core.metadata.security.auth.OAuthenticationInfo;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.security.OSecurityAuthenticator;
import com.orientechnologies.core.security.OSecuritySystem;
import javax.security.auth.Subject;

/**
 * Provides an abstract implementation of OSecurityAuthenticator.
 */
public abstract class OSecurityAuthenticatorAbstract implements OSecurityAuthenticator {

  private String name = "";
  private boolean debug = false;
  private boolean enabled = true;
  private boolean caseSensitive = true;
  private OSecuritySystem security;

  protected OSecuritySystem getSecurity() {
    return security;
  }

  protected boolean isDebug() {
    return debug;
  }

  protected boolean isCaseSensitive() {
    return caseSensitive;
  }

  // OSecurityComponent
  public void active() {
  }

  // OSecurityComponent
  public void config(YTDatabaseSessionInternal session, final YTEntityImpl jsonConfig,
      OSecuritySystem security) {
    this.security = security;
    if (jsonConfig != null) {
      if (jsonConfig.containsField("name")) {
        name = jsonConfig.field("name");
      }

      if (jsonConfig.containsField("debug")) {
        debug = jsonConfig.field("debug");
      }

      if (jsonConfig.containsField("enabled")) {
        enabled = jsonConfig.field("enabled");
      }

      if (jsonConfig.containsField("caseSensitive")) {
        caseSensitive = jsonConfig.field("caseSensitive");
      }
    }
  }

  // OSecurityComponent
  public void dispose() {
  }

  // OSecurityComponent
  public boolean isEnabled() {
    return enabled;
  }

  // OSecurityAuthenticator
  // databaseName may be null.
  public String getAuthenticationHeader(String databaseName) {
    String header;

    // Default to Basic.
    if (databaseName != null) {
      header = "WWW-Authenticate: Basic realm=\"YouTrackDB db-" + databaseName + "\"";
    } else {
      header = "WWW-Authenticate: Basic realm=\"YouTrackDB Server\"";
    }

    return header;
  }

  public Subject getClientSubject() {
    return null;
  }

  // Returns the name of this OSecurityAuthenticator.
  public String getName() {
    return name;
  }

  public YTSecurityUser getUser(final String username, YTDatabaseSessionInternal session) {
    return null;
  }

  public boolean isAuthorized(YTDatabaseSessionInternal session, final String username,
      final String resource) {
    return false;
  }

  @Override
  public YTSecurityUser authenticate(
      YTDatabaseSessionInternal session, OAuthenticationInfo authenticationInfo) {
    // Return null means no valid authentication
    return null;
  }

  public boolean isSingleSignOnSupported() {
    return false;
  }

  protected boolean isPasswordValid(YTDatabaseSessionInternal session, final YTSecurityUser user) {
    return user != null && user.getPassword(session) != null && !user.getPassword(session)
        .isEmpty();
  }
}
