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
package com.orientechnologies.orient.core.security.authenticator;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;

/**
 * Provides an OSecurityAuthenticator for the users listed in orientdb-server-config.xml.
 */
public class OServerConfigAuthenticator extends OSecurityAuthenticatorAbstract {

  // OSecurityComponent
  // Called once the Server is running.
  public void active() {
    OLogManager.instance().debug(this, "OServerConfigAuthenticator is active");
  }

  // OSecurityAuthenticator
  // Returns the actual username if successful, null otherwise.
  public OSecurityUser authenticate(
      YTDatabaseSessionInternal session, final String username, final String password) {
    return getSecurity().authenticateServerUser(session, username, password);
  }

  // OSecurityAuthenticator
  public OSecurityUser getUser(final String username, YTDatabaseSessionInternal session) {
    return getSecurity().getServerUser(session, username);
  }

  // OSecurityAuthenticator
  // If not supported by the authenticator, return false.
  public boolean isAuthorized(YTDatabaseSessionInternal session, final String username,
      final String resource) {
    return getSecurity().isServerUserAuthorized(session, username, resource);
  }

  // Server configuration users are never case sensitive.
  @Override
  protected boolean isCaseSensitive() {
    return false;
  }
}
