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
package com.jetbrains.youtrack.db.internal.core.security.authenticator;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORole;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORule;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurityRole;
import com.jetbrains.youtrack.db.internal.core.metadata.security.YTSecurityUser;

/**
 * Provides a default password authenticator.
 */
public class OSystemUserAuthenticator extends OSecurityAuthenticatorAbstract {

  // OSecurityComponent
  // Called once the Server is running.
  public void active() {
    LogManager.instance().debug(this, "OSystemUserAuthenticator is active");
  }

  // OSecurityComponent
  // Called on removal of the authenticator.
  public void dispose() {
  }

  // OSecurityAuthenticator
  // Returns the actual username if successful, null otherwise.
  // This will authenticate username using the system database.
  public YTSecurityUser authenticate(
      YTDatabaseSessionInternal session, final String username, final String password) {

    try {
      if (getSecurity() != null) {
        // dbName parameter is null because we don't need to filter any roles for this.
        YTSecurityUser user = getSecurity().getSystemUser(username, null);

        if (user != null && user.getAccountStatus(session) == YTSecurityUser.STATUSES.ACTIVE) {
          if (user.checkPassword(session, password)) {
            return user;
          }
        }
      }
    } catch (Exception ex) {
      LogManager.instance().error(this, "authenticate()", ex);
    }

    return null;
  }

  // OSecurityAuthenticator
  // If not supported by the authenticator, return false.
  // Checks to see if a
  public boolean isAuthorized(YTDatabaseSessionInternal session, final String username,
      final String resource) {
    if (username == null || resource == null) {
      return false;
    }

    try {
      if (getSecurity() != null) {
        YTSecurityUser user = getSecurity().getSystemUser(username, null);

        if (user != null && user.getAccountStatus(session) == YTSecurityUser.STATUSES.ACTIVE) {
          OSecurityRole role = null;

          ORule.ResourceGeneric rg = ORule.mapLegacyResourceToGenericResource(resource);

          if (rg != null) {
            String specificResource = ORule.mapLegacyResourceToSpecificResource(resource);

            if (specificResource == null || specificResource.equals("*")) {
              specificResource = null;
            }

            role = user.checkIfAllowed(session, rg, specificResource, ORole.PERMISSION_EXECUTE);
          }

          return role != null;
        }
      }
    } catch (Exception ex) {
      LogManager.instance().error(this, "isAuthorized()", ex);
    }

    return false;
  }

  // OSecurityAuthenticator
  public YTSecurityUser getUser(final String username, YTDatabaseSessionInternal session) {
    YTSecurityUser userCfg = null;

    try {
      if (getSecurity() != null) {
        userCfg = getSecurity().getSystemUser(username, null);
      }
    } catch (Exception ex) {
      LogManager.instance().error(this, "getUser()", ex);
    }

    return userCfg;
  }
}
